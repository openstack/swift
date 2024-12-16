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
import time
from collections import defaultdict

from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.middleware.mpu import MPU_DELETED_MARKER_SUFFIX, \
    MPU_MARKER_CONTENT_TYPE, MPU_GENERIC_MARKER_SUFFIX, MPUSession, MPUItem
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


class BaseMpuAuditor(object):
    resource_type = 'base'

    def __init__(self, config, client, logger, broker):
        self.config = config
        self.client = client
        self.logger = logger
        self.statsd_client = logger.logger.statsd_client
        self.stats = defaultdict(int)
        self.broker = broker
        self.uploads_already_checked = {}
        self.keep = {}

    def _dump_stats(self):
        return ', '.join(
            ['%s=%s' % (key, self.stats[key])
             for key in ('processed', 'audited', 'skipped', 'errors')])

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

    def info(self, fmt, *args):
        msg = fmt % args
        self.log(self.logger.info, msg)

    def debug(self, fmt, *args):
        msg = fmt % args
        self.log(self.logger.debug, msg)

    def increment(self, key):
        self.stats[key] += 1
        self.statsd_client.increment('%s.%s' % (self.resource_type, key))

    def _bump_item(self, item):
        # bump the item's *meta_timestamp* and merge to db so that the item is
        # moved to a new row; ctype_timestamp is not changed so any subsequent
        # updates to content-type from the middleware will not be prevented
        # from merging
        if item.deleted:
            item.deleted = 0
            item.data_timestamp = Timestamp(time.time())
        item.meta_timestamp = Timestamp(time.time())
        self.broker.put_record(item.to_db_record())

    def _audit_item(self, item, upload):
        raise NotImplementedError

    def audit(self):
        self.broker.get_info()
        max_row = self.broker.get_max_row()
        self.debug('visiting %s container %s',
                   self.resource_type, self.broker.path)
        context = MpuAuditorContext.load(self.broker)
        self.debug('auditing from row %s to row %s' %
                   (context.last_audit_row, max_row))
        for batch in yield_item_batches(self.broker,
                                        context.last_audit_row,
                                        self.config.max_batches,
                                        self.config.batch_size):
            for item_dict in batch:
                self.increment('processed')
                try:
                    item = MPUItem.from_db_record(item_dict)
                    self.debug('auditing %s %s',
                               self.resource_type, dict(item))
                    if not item.deleted:
                        upload = extract_upload_prefix(item.name)
                        if upload in self.uploads_already_checked:
                            self.increment('skipped')
                        else:
                            self._audit_item(item, upload)
                            self.uploads_already_checked[upload] = True
                            self.increment('audited')
                except Exception as err:  # noqa
                    self.exception('Error while auditing %s: %s',
                                   item_dict['name'], str(err))
                    self.increment('errors')
                    # TODO: hmmm, should we revisit?
                row_id = item_dict['ROWID']
                context.last_audit_row = row_id
                if row_id == max_row:
                    break
            else:
                continue
            break
        context.store(self.broker)
        self.info('%s', self._dump_stats())
        return None


class BaseMpuMarkerAuditor(BaseMpuAuditor):
    def _get_items_with_prefix(self, prefix, limit, include_deleted=False):
        # get results as dicts...
        # TODO: broker.get_objects doesn't support prefix so working around...
        transform_func = functools.partial(ContainerBroker._record_to_dict,
                                           self.broker)
        rows = self.broker.list_objects_iter(
            limit, '', '', prefix, None, include_deleted=include_deleted,
            transform_func=transform_func, allow_reserved=True)
        self.debug('get_items %s', rows)
        return [MPUItem.from_db_record(row) for row in rows]

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

    def _delete_marker(self, marker):
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
        self.debug('deleting item %s/%s',
                   self.broker.container, marker.name)
        self.client.delete_object(
            self.broker.account, self.broker.container, marker.name)

    def _process_marker(self, marker, upload):
        try:
            self._delete_resources(marker, upload)
        except Exception:  # noqa
            self._bump_item(marker)
        else:
            if not marker.deleted:
                try:
                    self._delete_marker(marker)
                except Exception:  # noqa
                    self._bump_item()

    def _audit_item(self, item, upload):
        if item.content_type == MPU_MARKER_CONTENT_TYPE:
            self.debug('found marker %s %s', item.name, item.content_type)
            self._process_marker(item, upload)
        else:
            self.debug('found resource %s', item.name)
            # Look for a potentially deleted marker in a previous or later row.
            # TODO: try to make this more efficient.
            #   If we find one we'll do another DB query to find all matching
            #   resources, but for manifest audit we already have the single
            #   expected manifest item. Also, when we expect multiple matching
            #   resources, we could just do one query now for all matches and
            #   look in the results for a marker.
            marker_prefix = '/'.join([upload, MPU_GENERIC_MARKER_SUFFIX])
            marker_item = self._get_item_with_prefix(marker_prefix,
                                                     include_deleted=None)
            if marker_item:
                self._process_marker(marker_item, upload)

        return False


class MpuManifestMarkerAuditor(BaseMpuMarkerAuditor):
    resource_type = 'manifest'

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
        # Write a delete-marker in the parts container so all parts will
        # eventually be deleted. Only if that succeeds, delete the manifest.
        self._put_parts_marker(upload)
        self.debug('deleting manifest %s', upload)
        self.client.delete_object(
            self.broker.account, self.broker.container, upload)


class MpuPartMarkerAuditor(BaseMpuMarkerAuditor):
    resource_type = 'part'

    def _find_orphans(self, marker, upload):
        # TODO: prefix query may scoop up some alien objects??
        #   need to check that each orphan is an MPU manifest
        return [
            item for item in
            self._get_items_with_prefix(upload, limit=1000)
            if item.name != marker.name]

    def _delete_resources(self, marker, upload):
        orphan_parts = self._find_orphans(marker, upload)
        err_to_raise = None
        for orphan in orphan_parts:
            self.debug('deleting part %s', orphan.name)
            try:
                self.client.delete_object(self.broker.account,
                                          self.broker.container,
                                          orphan.name)
            except UnexpectedResponse as err:
                # keep going for now...
                err_to_raise = err

        if err_to_raise:
            raise err_to_raise


class MpuSessionAuditor(BaseMpuAuditor):
    resource_type = 'session'

    def __init__(self, conf, client, logger, broker):
        super(MpuSessionAuditor, self).__init__(
            conf, client, logger, broker)
        # TODO: move these attributes to super-class, but that will require
        #   superclass to only deal with reserved namespace containers
        self.user_container = split_reserved_name(self.broker.container)[1]
        self.manifests_container = get_reserved_name('mpu_manifests',
                                                     self.user_container)

    def _is_manifest_linked(self, upload):
        """
        :raises UnexpectedResponse: if it is not possible to reliably confirm
            if the user object is a symlink to the given upload.
        :return: True if the user object is a symlink to this target, False if
            the user object is not a symlink to the given upload.
        """
        obj_name = extract_object_name(upload)
        user_container = split_reserved_name(self.broker.container)[1]
        metadata = self.client.get_object_metadata(
            self.broker.account, user_container, obj_name,
            headers={'X-Newest': 'true'},
            acceptable_statuses=(2, 404))
        link = metadata.get(TGT_OBJ_SYMLINK_HDR)
        manifests_container = get_reserved_name('mpu_manifests',
                                                user_container)
        relative_manifest_path = '/'.join([manifests_container, upload])
        return link == relative_manifest_path

    def _delete_session(self, session):
        self.client.delete_object(self.broker.account,
                                  self.broker.container,
                                  session.name)

    def _put_manifest_delete_marker(self, upload):
        # TODO: maybe share some common helper functions with middleware
        marker_name = '/'.join([upload, MPU_DELETED_MARKER_SUFFIX])
        self.debug('putting marker %s/%s',
                   self.manifests_container, marker_name)
        self.client.upload_object(
            None, self.broker.account, self.manifests_container, marker_name,
            headers={'Content-Type': MPU_DELETED_MARKER_SUFFIX,
                     'Content-Length': '0',
                     'X-Backend-Allow-Reserved-Names': 'true'}
        )

    def _audit_completing_session(self, session, upload):
        now = time.time()
        ctype_age = now - float(session.ctype_timestamp)
        try:
            if self._is_manifest_linked(upload):
                session.set_completed(Timestamp(now))
                return
        except UnexpectedResponse:
            self._bump_item(session)
            return

        if ctype_age < self.config.completing_period:
            # completeUpload may still be in progress: recheck later, but
            # not forever
            self._bump_item(session)
        else:
            # the completeUpload presumably failed: leave for user to retry
            # or abortUpload, either of which will cause the session to
            # appear in a new row
            pass

    def _audit_aborted_session(self, session, upload):
        now = time.time()
        ctype_age = now - float(session.ctype_timestamp)
        try:
            if self._is_manifest_linked(upload):
                self.debug('aborted session appears to have completed')
                session.set_completed(Timestamp(now))
                return
        except UnexpectedResponse:
            # can't be certain about the symlink so defer and revisit
            self._bump_item(session)
            return

        if ctype_age > self.config.purge_delay:
            # time to clean-up everything
            try:
                self.debug('deleting aborted session %s', session.name)
                # Write down an audit-marker in the manifests container
                # that will cause the auditor to check the status of the
                # mpu and possibly cleanup the manifest and parts.
                # TODO: maybe just delete the manifest and have the manifest
                # tombstone act as a "delete marker" that triggers the auditor
                # to delete parts. Do we need the manifest any more?
                # TODO: maybe write a parts delete marker here too?
                self._put_manifest_delete_marker(upload)
                self._delete_session(session)
            except Exception as err:  # noqa
                self.warning('Failed to delete aborted session %s: %s',
                             session.name, err)
                self._bump_item(session)
        else:
            # recheck later
            self.debug('aborted session deferred (age=%s)', ctype_age)
            self._bump_item(session)

    def _audit_completed_session(self, session, upload):
        try:
            # TODO: There may be parts that were uploaded but not referenced by
            #   the user's manifest. Figure out a way to mark parts that are
            #   NOT in the manifest to be deleted.
            self.debug('deleting completed session %s', session.name)
            self._delete_session(session)
        except Exception as err:  # noqa
            self.warning('Failed to prune completed session %s: %s',
                         session.name, err)
            self._bump_item(session)

    def _audit_item(self, item, upload):
        session = MPUSession(**dict(item))
        if session.is_completing:
            self._audit_completing_session(session, upload)
        elif session.is_active:
            # no need to bump item because the session will appear in a new row
            # when its state is updated by middleware
            pass
        elif session.is_aborted:
            self._audit_aborted_session(session, upload)

        if session.is_completed:
            self._audit_completed_session(session, upload)


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
        if reserved_prefix == 'mpu_parts' or broker.path.endswith('+segments'):
            mpu_auditor_class = MpuPartMarkerAuditor
        elif reserved_prefix == 'mpu_manifests':
            mpu_auditor_class = MpuManifestMarkerAuditor
        elif reserved_prefix == 'mpu_sessions':
            mpu_auditor_class = MpuSessionAuditor
        else:
            mpu_auditor_class = None

        if mpu_auditor_class:
            mpu_auditor_class = mpu_auditor_class(
                self.config, self.client, self.logger, broker)
            mpu_auditor_class.audit()