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
    MPU_MARKER_CONTENT_TYPE, MPUSession, MPUItem, MPU_SYSMETA_UPLOAD_ID_KEY
from swift.common.object_ref import ObjectRef, HistoryId, UploadId
from swift.common.request_helpers import split_reserved_name, get_reserved_name
from swift.common.utils import Timestamp, get_logger, non_negative_float, \
    non_negative_int, config_positive_int_value, param_str_to_dict
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


class MpuAuditorContext:
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


class MpuAuditorConfig:
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


class BaseMpuAuditor:
    resource_type = 'base'

    def __init__(self, config, client, logger, broker, user_container):
        self.config = config
        self.client = client
        self.logger = logger
        self.broker = broker
        self.user_container = user_container
        self.parts_container = get_reserved_name('mpu_parts',
                                                 self.user_container)
        self.statsd_client = logger.logger.statsd_client
        self.stats = defaultdict(int)
        self.already_checked = {}

    def _dump_stats(self):
        return ', '.join(
            ['%s=%s' % (key, self.stats[key])
             for key in ('processed', 'audited', 'skipped', 'errors')])

    def log(self, log_func, msg):
        data = {
            'msg': msg,
            'path': self.broker.path,
            'db_file': self.broker.db_file,
        }
        log_func('mpu-auditor (%s) %s', self.resource_type, json.dumps(data))

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

    def _put_delete_marker(self, obj):
        account = self.broker.account.lstrip('.')
        self.debug('putting marker %s/%s/%s',
                   account, self.parts_container, obj)
        self.client.upload_object(
            None, account, self.parts_container, obj,
            headers={'Content-Type': MPU_MARKER_CONTENT_TYPE,
                     'Content-Length': '0',
                     'X-Backend-Allow-Reserved-Names': 'true'}
        )

    def _get_items_with_prefix(self, prefix, limit, include_deleted=False):
        # get results as dicts...
        # TODO: broker.get_objects doesn't support prefix so working around...
        transform_func = functools.partial(ContainerBroker._record_to_dict,
                                           self.broker)
        rows = self.broker.list_objects_iter(
            limit, '', '', prefix, None, include_deleted=include_deleted,
            transform_func=transform_func, allow_reserved=True)
        self.debug('get_items_with_prefix %s (%d): %s',
                   prefix, len(rows), rows)
        return [MPUItem.from_db_record(row) for row in rows]

    def _get_item_with_prefix(self, name, include_deleted=False):
        self.debug('get_item prefix %s', name)
        items = self._get_items_with_prefix(
            name, limit=1, include_deleted=include_deleted)
        if items:
            return items[0]
        return None

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

    def _bump_item_meta_offset(self, item):
        item.meta_timestamp.offset += 1
        self.broker.put_record(item.to_db_record())

    def _delete_item(self, item):
        item.deleted = 1
        item.data_timestamp.offset += 1
        self.broker.put_record(item.to_db_record())

    def _audit_item(self, item, upload):
        raise NotImplementedError

    def audit(self):
        self.broker.get_info()
        max_row = self.broker.get_max_row()
        self.debug('visiting container %s', self.broker.path)
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
                    self.debug('auditing item %s', dict(item))
                    if not item.deleted:
                        obj_ref = ObjectRef.parse(item.name)
                        obj_ref_prefix = obj_ref.serialize(drop_tail=True)
                        if obj_ref_prefix in self.already_checked:
                            self.increment('skipped')
                        else:
                            self._audit_item(item, obj_ref)
                            self.already_checked[obj_ref_prefix] = True
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


class MpuHistoryAuditor(BaseMpuAuditor):
    resource_type = 'history'

    def _cleanup_obsolete_mpu(self, item, obj_ref, mpu_policy):
        # TODO: vary parts container according to policy OR put explicit path
        #  to parts in systags
        history_id = HistoryId.parse(obj_ref.obj_id)
        upload_id = UploadId(history_id.timestamp)
        marker_ref = ObjectRef(obj_ref.user_name,
                               upload_id.serialize(),
                               tail=MPU_DELETED_MARKER_SUFFIX)
        try:
            self._put_delete_marker(marker_ref.serialize())
        except Exception as err:  # noqa
            self.warning('Failed to put delete marker to %s/%s: %s',
                         self.parts_container, str(marker_ref), err)
            # move to a new row to ensure it is revisited
            self._bump_item_meta_offset(item)
        else:
            self.increment('marked')
            self._delete_item(item)

    def _cleanup_obsolete_item(self, item, obj_ref):
        if item.deleted:
            return
        self.debug('cleaning up obsolete item %s', item.name)
        mpu_policy = param_str_to_dict(item.systags).get('mpu_policy')
        if mpu_policy is None:
            self._delete_item(item)
        elif obj_ref.tail == 'DELETE':
            self._delete_item(item)
        else:
            self._cleanup_obsolete_mpu(item, obj_ref, mpu_policy)

    def _audit_null_version(self, items):
        for i, item in enumerate(items):
            if item.deleted:
                continue

            obj_ref = ObjectRef.parse(item.name)
            if i == 0:
                # latest version may be a deletion
                if obj_ref.tail == 'DELETE':
                    # the delete version tombstone row will linger for reclaim
                    # age in case other rows turn up for older versions.
                    self._delete_item(item)
                continue

            # older versions are cleaned up
            self._cleanup_obsolete_item(item, obj_ref)

    def _audit_retained_version(self, items):
        self.debug('_audit_retained_version %s', [item.name for item in items])
        item_obj_refs = [(item, ObjectRef.parse(item.name)) for item in items]
        if any(obj_ref.tail == 'DELETE' for item, obj_ref in item_obj_refs):
            for item, obj_ref in item_obj_refs:
                self._cleanup_obsolete_item(item, obj_ref)

    def _audit_item(self, item, obj_ref):
        # TODO: iterate over batches...
        obj_id = HistoryId.parse(obj_ref.obj_id)
        prefix_obj_ref = ObjectRef(obj_ref.user_name,
                                   obj_id.serialize(prefix_only=True))
        item_versions = self._get_items_with_prefix(
            prefix_obj_ref.serialize(), limit=1000, include_deleted=None)
        if not item_versions:
            # possible if racing with replicator?
            return

        if obj_id.null:
            self._audit_null_version(item_versions)
        else:
            self._audit_retained_version(item_versions)


class MpuPartMarkerAuditor(BaseMpuAuditor):
    resource_type = 'part'

    def _delete_marker(self, marker_item):
        # TODO: do we do this even if we didn't find any part rows yet?
        # Delete the marker now so that it will eventually be reclaimed. We
        # can still find the marker row for subsequent checks until it is
        # reclaimed.
        # Note: Delete the marker by applying an offset to its data_timestamp.
        # There may be a version of the marker at a later data_timestamp which
        # has not yet been merged into this DB and must be merged and processed
        # independently, for example a marker due to a version object being
        # deleted.
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
                   self.broker.container, marker_item.name)
        ts_delete = marker_item.data_timestamp
        ts_delete.offset += 1
        headers = {'X-Timestamp': ts_delete.internal}
        self.client.delete_object(
            self.broker.account, self.broker.container, marker_item.name,
            headers=headers)

    def _find_orphans(self, marker_item, marker_name):
        # TODO: prefix query may scoop up some alien objects??
        #   need to check that each orphan is an MPU resource
        prefix = marker_name.serialize(drop_tail=True)
        return [
            item for item in
            self._get_items_with_prefix(prefix, limit=1000)
            if item.name != marker_item.name]

    def _delete_resources(self, marker_item, marker_name):
        orphan_parts = self._find_orphans(marker_item, marker_name)
        err_to_raise = None
        for orphan in orphan_parts:
            self.debug('deleting part %s', orphan.name)
            self.increment('deleted')
            try:
                self.client.delete_object(self.broker.account,
                                          self.broker.container,
                                          orphan.name)
            except UnexpectedResponse as err:
                # keep going for now...
                err_to_raise = err

        if err_to_raise:
            raise err_to_raise

    def _process_marker(self, marker_item, marker_name):
        try:
            self._delete_resources(marker_item, marker_name)
        except UnexpectedResponse:
            self._bump_item(marker_item)
        except Exception as err:  # noqa
            self.exception('Error deleting resources: %s', err)
            self._bump_item(marker_item)
        else:
            if not marker_item.deleted:
                try:
                    self._delete_marker(marker_item)
                except Exception as err:  # noqa
                    self.warning('Error deleting marker: %s', err)
                    self._bump_item(marker_item)

    def _audit_item(self, item, obj_ref):
        # split name/<part_number> or name/delete-marker
        if obj_ref.tail == MPU_DELETED_MARKER_SUFFIX:
            self.debug('found marker %s %s', item.name, item.content_type)
            self.increment('marker')
            self._process_marker(item, obj_ref)
        else:
            self.debug('found resource %s', item.name)
            # Look for a potentially deleted marker in a previous or later row.
            # TODO: try to make this more efficient.
            #   If we find one we'll do another DB query to find all matching
            #   resources, but for manifest audit we already have the single
            #   expected manifest item. Also, when we expect multiple matching
            #   resources, we could just do one query now for all matches and
            #   look in the results for a marker.
            marker_ref = obj_ref.clone()
            marker_ref.tail = MPU_DELETED_MARKER_SUFFIX
            marker_item = self._get_item_with_prefix(str(marker_ref),
                                                     include_deleted=None)
            if marker_item:
                self._process_marker(marker_item, marker_ref)

        return False


class MpuSessionAuditor(BaseMpuAuditor):
    resource_type = 'session'

    def __init__(self, conf, client, logger, broker, user_container):
        super().__init__(conf, client, logger, broker, user_container)

    def _is_mpu_in_user_namespace(self, obj_ref):
        """
        :raises UnexpectedResponse: if it is not possible to reliably confirm
            if the user object is a manifest for the given upload.
        :return: True if the user object is a manifest for the given upload,
            False if the user object is not a manifest for the given upload.
        """
        metadata = self.client.get_object_metadata(
            self.broker.account, self.user_container, obj_ref.user_name,
            headers={'X-Newest': 'true'},
            acceptable_statuses=(2, 404))
        return metadata.get(MPU_SYSMETA_UPLOAD_ID_KEY) == obj_ref.obj_id

    def _delete_session(self, session):
        self.client.delete_object(self.broker.account,
                                  self.broker.container,
                                  session.name)

    def _audit_completing_session(self, session, obj_ref):
        now = time.time()
        ctype_age = now - float(session.ctype_timestamp)
        try:
            if self._is_mpu_in_user_namespace(obj_ref):
                # TODO: check versions container too
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

    def _audit_aborted_session(self, session, obj_ref):
        now = time.time()
        ctype_age = now - float(session.ctype_timestamp)
        try:
            if self._is_mpu_in_user_namespace(obj_ref):
                self.debug('aborted session appears to have completed')
                session.set_completed(Timestamp(now))
                return
        except UnexpectedResponse:
            # can't be certain about the symlink so defer and revisit
            self._bump_item(session)
            return

        if ctype_age > self.config.purge_delay:
            # time to clean-up everything
            marker_ref = obj_ref.clone()
            marker_ref.tail = MPU_DELETED_MARKER_SUFFIX
            try:
                self.debug('deleting aborted session %s', session.name)
                self._put_delete_marker(marker_ref.serialize())
                self._delete_session(session)
            except Exception as err:  # noqa
                self.warning('Failed to delete aborted session %s: %s',
                             session.name, err)
                self._bump_item(session)
        else:
            # recheck later
            self.debug('aborted session deferred (age=%s)', ctype_age)
            self._bump_item(session)

    def _audit_completed_session(self, session, obj_ref):
        try:
            self.debug('deleting completed session %s', session.name)
            self._delete_session(session)
        except Exception as err:  # noqa
            self.warning('Failed to prune completed session %s: %s',
                         session.name, err)
            self._bump_item(session)

    def _audit_item(self, item, obj_ref):
        session = MPUSession(**dict(item))
        if session.is_completing:
            self._audit_completing_session(session, obj_ref)
        elif session.is_active:
            # no need to bump item because the session will appear in a new row
            # when its state is updated by middleware
            pass
        elif session.is_aborted:
            self._audit_aborted_session(session, obj_ref)

        if session.is_completed:
            self._audit_completed_session(session, obj_ref)


class MpuAuditor:
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
        if reserved_prefix == 'mpu_parts':
            mpu_auditor_class = MpuPartMarkerAuditor
            user_container = container
        elif reserved_prefix == 'mpu_sessions':
            mpu_auditor_class = MpuSessionAuditor
            user_container = container
        elif reserved_prefix == 'history':
            mpu_auditor_class = MpuHistoryAuditor
            user_container = container
        elif broker.path.endswith('+segments'):
            mpu_auditor_class = MpuPartMarkerAuditor
            user_container = broker.container[:-1 * len('+segments')]
        else:
            return

        mpu_auditor = mpu_auditor_class(
            self.config, self.client, self.logger, broker, user_container)
        mpu_auditor.audit()
