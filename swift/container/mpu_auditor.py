# Copyright (c) 2024 Nvidia
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import functools
import json

from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.middleware.mpu import MPU_DELETED_MARKER_SUFFIX, \
    MPU_ABORTED_MARKER_SUFFIX, MPU_MARKER_CONTENT_TYPE, \
    MPU_GENERIC_MARKER_SUFFIX
from swift.common.middleware.symlink import TGT_OBJ_SYMLINK_HDR
from swift.common.request_helpers import split_reserved_name, get_reserved_name
from swift.common.utils import Timestamp, get_logger, non_negative_float, \
    non_negative_int, config_positive_int_value
from swift.container.backend import ContainerBroker


# TODO: share with object_versioning?  move to request_helpers?
def safe_split_reserved_name(reserved_name):
    # TODO: this assumes 2 parts, should it be generalised
    try:
        return split_reserved_name(reserved_name)
    except ValueError:
        return None, reserved_name


def yield_item_batches(broker, start_row, max_batches, batch_size):
    remaining = max_batches * batch_size
    while remaining > 0:
        batch_limit = min(batch_size, remaining)
        items = broker.get_items_since(start_row, batch_limit)
        if items:
            remaining -= len(items)
            start_row = items[-1]['ROWID']
            yield items
        else:
            remaining = 0


class Item(object):
    def __init__(self, name, created_at, size=0, content_type='',
                 etag='', deleted=0, **kwargs):
        self._name = name
        self.timestamp = Timestamp(created_at)
        self.size = size
        self.content_type = content_type
        self.etag = etag
        self.deleted = deleted
        self.kwargs = kwargs

    @property
    def name(self):
        return str(self._name)

    @property
    def user_name(self):
        return self._name.name

    def __iter__(self):
        yield 'name', str(self.name)
        yield 'created_at', self.timestamp.internal
        yield 'size', self.size
        yield 'etag', self.etag
        yield 'deleted', self.deleted
        yield 'content_type', self.content_type
        for k, v in self.kwargs.items():
            yield k, v


def extract_upload_prefix(name):
    """
    Return the upload prefix from a given object name.
    """
    parts = name.strip('/').rsplit('/', 2)
    return '/'.join(parts[:2])


def extract_object_name(name):
    parts = name.strip('/').rsplit('/', 2)
    return split_reserved_name(parts[0])[0]


class MpuAuditorContext(object):
    """
    Encapsulate state related to the auditor's visits to a container DB.

    A serialized representation of this state is stored as metadata in the DB.
    """
    MPU_AUDITOR_SYSMETA_PREFIX = 'X-Container-Sysmeta-Mpu-Auditor-'

    def __init__(self, ref, last_audit_row=0):
        self.ref = ref
        self.last_audit_row = last_audit_row

    def __iter__(self):
        yield 'ref', self.ref
        yield 'last_audit_row', self.last_audit_row

    @classmethod
    def _make_sysmeta_key(cls, broker):
        prefix = 'X-Container-Sysmeta-Mpu-Auditor-Context-'
        return prefix + broker.get_info()['id']

    @classmethod
    def load(cls, broker):
        """
        :param broker: an instances of :class:`ContainerBroker`
        :return: An instance of :class:`MpuAuditorContext`.
        """
        ref = cls._make_sysmeta_key(broker)
        data, _ = broker.metadata.get(cls._make_sysmeta_key(broker), (None, 0))
        data = json.loads(data) if data else {}
        data['ref'] = ref
        return cls(**data)

    def store(self, broker):
        """
        :param broker: an instances of :class:`ContainerBroker`
        """
        broker.update_metadata(
            {self._make_sysmeta_key(broker):
             (json.dumps(dict(self)), Timestamp.now().internal)})


class MpuAuditorConfig(object):
    def __init__(self, conf):
        # the auditor will not purge an aborted MPU's resources until it has
        # been in the aborted state for more than mpu_aborted_purge_delay
        self.purge_delay = non_negative_float(
            conf.get('mpu_aborted_purge_delay', 86400))
        # the auditor will check if a completing MPU has in fact been completed
        # been in the completed state for more than mpu_completing_period
        self.completing_period = non_negative_float(
            conf.get('mpu_completing_period', 3600))
        # the auditor will select up to mpu_audit_batch_size rows with each
        # container DB query
        self.batch_size = config_positive_int_value(
            conf.get('mpu_audit_batch_size', 1000))
        # the auditor will make up to mpu_audit_max_batches container DB
        # queries during each visit to a container DB
        self.max_batches = non_negative_int(
            conf.get('mpu_audit_max_batches', 100))


class BaseMpuBrokerAuditor(object):
    def __init__(self, config, client, logger, broker, resource_type):
        self.config = config
        self.client = client
        self.logger = logger
        self.broker = broker
        self.uploads_already_checked = {}
        self.keep = {}
        self.resource_type = resource_type

    def log(self, log_func, msg):
        data = {
            'msg': msg,
            'db_file': self.broker.db_file,
            'path': self.broker.path,
        }
        log_func('mpu-auditor ' + json.dumps(data))

    def exception(self, fmt, *args):
        msg = fmt % args
        self.log(self.logger.exception, msg)

    def warning(self, fmt, *args):
        msg = fmt % args
        self.log(self.logger.warning, msg)

    def debug(self, fmt, *args):
        msg = fmt % args
        self.log(self.logger.debug, msg)

    def _get_items_with_prefix(self, prefix, limit, include_deleted=False):
        # get results as dicts...
        # TODO: broker.get_objects doesn't support prefix so working around...
        transform_func = functools.partial(ContainerBroker._record_to_dict,
                                           self.broker)
        rows = self.broker.list_objects_iter(
            limit, '', '', prefix, None, include_deleted=include_deleted,
            transform_func=transform_func, allow_reserved=True)
        self.debug('get_items %s', rows)
        return [Item(**row) for row in rows]

    def _get_item_with_prefix(self, name, include_deleted=False):
        self.debug('get_item %s', name)
        items = self._get_items_with_prefix(
            name, limit=1, include_deleted=include_deleted)
        if items:
            return items[0]
        return None

    def _delete_resources(self, marker, upload):
        # subclasses must implement this method
        raise NotImplementedError()

    def _process_marker(self, marker, upload):
        self._delete_resources(marker, upload)

        if not marker.deleted:
            # TODO: do we do this even if we didn't find any manifest row yet?
            # Delete the marker now so that it will eventually be reclaimed. We
            # can still find the marker row for subsequent checks until it is
            # reclaimed.
            # Note: Deleting a marker with timestamp t0 moves it's tombstone
            # timestamp forwards to t2. There may also be a version of the
            # marker at t1 which has not yet been merged into this DB. This
            # delete at t2 will supersede that marker at t1, but that's ok
            # because we don't care about the marker's timestamp or deleted
            # status, we just need a marker row whose name matches a resource.
            # Note: This deletion will be replicated to other DB replicas which
            # may have not yet had an undeleted marker row. The auditor may
            # have already passed over the matching resource row in the other
            # DBs, and so will never detect the marked resource, because the
            # auditor only inspects rows once and does not inspect deleted
            # rows. This is OK because the matching resource was either
            # detected by the auditor on this cycle of this DB, or it will be
            # replicated to this DB from the other DBs and detected on a
            # subsequent cycle of this DB.
            # TODO: we could make the auditor process deleted marker rows so
            #   that other DBs in the state described in the above note would
            #   detect the marked resource. That would also mean that auditors
            #   might process each marker twice in each DB, particularly this
            #   DB: first as an undeleted row and then in a subsequent cycle as
            #   a deleted row. Apart from incurring extra work, that would be
            #   ok. I'm not yet sure if that redundancy is necessary, or
            #   desirable.
            self.debug('deleting marker %s/%s',
                       self.broker.container, marker.name)
            self.client.delete_object(
                self.broker.account, self.broker.container, marker.name)

    def _process_item(self, item, upload):
        if item.content_type == MPU_MARKER_CONTENT_TYPE:
            self.debug('found_marker %s %s', item.name, item.content_type)
            self._process_marker(item, upload)
        else:
            # TODO: try to make this more efficient. We need to look for a
            #   potentially deleted marker of either deleted or aborted type.
            #   If we find one we'll do another DB query to find all matching
            #   resources, but for manifest audit we already have the single
            #   expected manifest item. Also, when we expect multiple matching
            #   resources, we could just do one query now for all matches and
            #   look in the results for a marker.
            self.debug('found_resource %s', item.name)
            marker_prefix = '/'.join([upload, MPU_GENERIC_MARKER_SUFFIX])
            # TODO: if there's an aborted *and* deleted marker then it would be
            #   more efficient to process the more definite deleted marker
            marker_item = self._get_item_with_prefix(marker_prefix,
                                                     include_deleted=None)
            if marker_item:
                self.debug('found_marker_for_resource %s', marker_item.name)
                self._process_marker(marker_item, upload)

    def _audit_item(self, item):
        self.debug('item %s %s', item.name, item.deleted)
        upload = extract_upload_prefix(item.name)
        if upload in self.uploads_already_checked:
            return
        self._process_item(item, upload)
        self.uploads_already_checked[upload] = True

    def audit(self):
        self.broker.get_info()
        max_row = self.broker.get_max_row()
        self.debug('visiting %s container %s',
                   self.resource_type, self.broker.path)
        context = MpuAuditorContext.load(self.broker)
        self.debug('auditing from row %s to row %s' %
                   (context.last_audit_row, max_row))
        num_audited = num_processed = num_errors = num_skipped = 0
        for batch in yield_item_batches(self.broker,
                                        context.last_audit_row,
                                        self.config.max_batches,
                                        self.config.batch_size):
            for item_dict in batch:
                num_processed += 1
                try:
                    item = Item(**item_dict)
                    self.debug('auditing %s %s',
                               self.resource_type, dict(item))
                    if not item.deleted:
                        self._audit_item(item)
                        num_audited += 1
                except Exception as err:  # noqa
                    self.exception('Error while auditing %s: %s',
                                   item_dict['name'], str(err))
                    num_errors += 1
                    # TODO: hmmm, should we revisit?
                row_id = item_dict['ROWID']
                context.last_audit_row = row_id
                if row_id == max_row:
                    break
            else:
                continue
            break
        context.store(self.broker)
        self.debug('processed: %d, audited: %d, skipped: %d, errors: %d)',
                   num_processed, num_audited, num_skipped, num_errors)
        return None


class MpuManifestAuditor(BaseMpuBrokerAuditor):
    def _is_manifest_linked(self, marker, upload):
        obj_name = extract_object_name(upload)
        user_container = split_reserved_name(self.broker.container)[1]
        try:
            metadata = self.client.get_object_metadata(
                self.broker.account, user_container, obj_name,
                headers={'X-Newest': 'true'},
                acceptable_statuses=(2, 404))
        except UnexpectedResponse:
            # play it safe
            return False
        link = metadata.get(TGT_OBJ_SYMLINK_HDR)
        return link == '/'.join((self.broker.container, upload))

    def _delete_manifest_parts(self, upload):
        # TODO: add error handling!
        # there is no SLO in internal client so we have to fetch the manifest
        # and delete the parts here
        try:
            status, hdrs, resp_iter = self.client.get_object(
                self.broker.account, self.broker.container, upload,
                params={'multipart-manifest': 'get'})
        except UnexpectedResponse:
            # it's ok, this was just an optimisation, we'll PUT a delete marker
            # in the parts container anyway
            return

        # TODO: be defensive - check it is a manifest!
        manifest = json.loads(b''.join(resp_iter))
        for item in manifest:
            self.debug('deleting part %s', item['name'])
            container, obj = item['name'].lstrip('/').split('/', 1)
            try:
                self.client.delete_object(
                    self.broker.account, container, obj)
            except UnexpectedResponse:
                pass

    def _put_parts_marker(self, upload):
        # TODO: share some common helper functions with middleware
        user_container = split_reserved_name(self.broker.container)[1]
        parts_container = get_reserved_name('mpu_parts', user_container)
        marker_name = '/'.join([upload, MPU_DELETED_MARKER_SUFFIX])
        self.debug('putting marker %s/%s', parts_container, marker_name)
        self.client.upload_object(
            None, self.broker.account, parts_container, marker_name,
            headers={'Content-Type': MPU_DELETED_MARKER_SUFFIX,
                     'Content-Length': '0',
                     'X-Backend-Allow-Reserved-Names': 'true'}
        )

    def _delete_resources(self, marker, upload):
        # TODO: add a time delay before processing an abort to allow for
        # concurrent complete
        # TODO: handle failed requests
        if (marker.name.endswith(MPU_ABORTED_MARKER_SUFFIX) and
                self._is_manifest_linked(marker, upload)):
            # User object is linked -> no action required.
            # This marker will now be deleted, but may be processed again if
            # the manifest row is found in a subsequent auditor cycle, but that
            # should only happen once.
            # TODO: somehow prevent a repeat processing of this marker,
            #   possibly by playing tricks with the timestamp offset
            return
        # TODO: if the session entered completing state, check that a
        #   reasonable amount of time has passed since then; if not, re-PUT the
        #   marker for the next cycle to try again.
        # Attempt to sync delete the manifest's parts. This is an optional
        # optimisation; the manifest may not be in the DB yet, and there may
        # never be a manifest if the upload was aborted.
        self._delete_manifest_parts(upload)
        # Also write a delete marker in the parts container so all parts will
        # eventually be deleted anyway.
        self._put_parts_marker(upload)
        self.debug('deleting manifest %s', upload)
        self.client.delete_object(
            self.broker.account, self.broker.container, upload)


class MpuPartAuditor(BaseMpuBrokerAuditor):
    def _find_orphans(self, marker, upload):
        # TODO: prefix query may scoop up some alien objects??
        #   need to check that each orphan is an MPU manifest
        return [
            item for item in
            self._get_items_with_prefix(upload, limit=1000)
            if item.name != marker.name]

    def _delete_resources(self, marker, upload):
        orphan_parts = self._find_orphans(marker, upload)
        for orphan in orphan_parts:
            self.debug('deleting part %s', orphan.name)
            self.client.delete_object(self.broker.account,
                                      self.broker.container,
                                      orphan.name)


class MpuSessionAuditor(BaseMpuBrokerAuditor):
    def _process_item(self, item, upload):
        # if aborted -> abort -> delete
        # elif completed and linked -> delete
        # else leave it alone
        pass


class MpuAuditor(object):
    def __init__(self, conf, logger=None):
        self.config = MpuAuditorConfig(conf)
        self.logger = logger or get_logger({}, 'mpu-auditor')
        internal_client_conf_path = conf.get('internal_client_conf_path',
                                             '/etc/swift/internal-client.conf')
        self.client = InternalClient(
            internal_client_conf_path,
            'Swift Container Auditor',
            1,
            use_replication_network=True,
            global_conf={'log_name': '%s-ic' % conf.get(
                'log_name', 'container-auditor')})

    def audit(self, broker):
        reserved_prefix, container = safe_split_reserved_name(broker.container)
        self.logger.debug('mpu_audit visiting %s %s',
                          reserved_prefix, container)
        if reserved_prefix == 'mpu_parts' or broker.path.endswith('+segments'):
            mpu_auditor = MpuPartAuditor(
                self.config, self.client, self.logger, broker, 'part')
            return mpu_auditor.audit()
        if reserved_prefix == 'mpu_manifests':
            mpu_auditor = MpuManifestAuditor(
                self.config, self.client, self.logger, broker, 'manifest')
            return mpu_auditor.audit()
        elif reserved_prefix == 'mpu_sessions':
            mpu_auditor = MpuSessionAuditor(
                self.config, self.client, self.logger, broker, 'session')
            return mpu_auditor.audit()
        else:
            return None
