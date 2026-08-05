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
from urllib.parse import unquote
from collections import defaultdict

from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.middleware.mpu import MPUSession, MPUItem, \
    MPU_SYSMETA_UPLOAD_ID_KEY, parse_hidden_container_name, \
    make_parts_container_name, extract_user_account_name, \
    make_mpu_hidden_account_name, is_mpu_hidden_account_name
from swift.common.middleware.versioned_writes.object_versioning import \
    build_versions_container_name, build_versions_object_name
from swift.common.object_ref import ObjectRef, UploadId
from swift.common.request_helpers import split_reserved_name
from swift.common.utils import Timestamp, get_logger, non_negative_float, \
    non_negative_int, config_positive_int_value
from swift.container.backend import ContainerBroker


def _safe_get_and_unquote(map, key, default_value=None):
    # TODO: add unit test coverage
    value = map.get(key)
    if value:
        return unquote(value)
    else:
        return default_value


def yield_item_batches(broker, max_batches, batch_size, include_states, table):
    remaining = max_batches * batch_size
    marker = None
    while remaining > 0:
        batch_limit = min(batch_size, remaining)
        items = broker.get_objects(
            marker=marker, limit=batch_limit, include_states=include_states,
            table=table
        )
        if items:
            remaining -= len(items)
            marker = items[-1]['name']
            yield items
        else:
            remaining = 0


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
    audit_table = 'action'
    audit_states = {0}

    def __init__(self, config, client, logger, broker, user_account,
                 user_container):
        self.config = config
        self.client = client
        self.logger = logger
        self.broker = broker
        self.user_account = user_account
        self.user_container = user_container
        self.hidden_account = make_mpu_hidden_account_name(self.user_account)
        # TODO: parts container may vary per mpu w.r.t. policy
        self.parts_container = make_parts_container_name(self.user_container)
        self.statsd_client = logger.logger.statsd_client
        self.stats = defaultdict(int)
        self.ts_audit = Timestamp.now()
        self.completed_items = None

    def _dump_stats(self):
        return ', '.join(
            ['%s=%s' % (key, self.stats[key])
             for key in ('processed', 'audited', 'errors')])

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

    def _get_items_with_prefix(self, prefix, limit, include_states=None):
        # get results as dicts...
        # TODO: broker.get_objects doesn't support prefix so working around...
        transform_func = functools.partial(ContainerBroker._record_to_dict,
                                           self.broker)
        rows = self.broker.list_objects_iter(
            limit, '', '', prefix, None, include_states=include_states,
            transform_func=transform_func, allow_reserved=True)
        self.debug('get_items_with_prefix %s (%d): %s',
                   prefix, len(rows), rows)
        return [MPUItem.from_db_record(row) for row in rows]

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

    def _complete_item(self, item, ts=None):
        item.deleted = 1
        if ts:
            item.data_timestamp = ts
        else:
            item.data_timestamp.offset += 1
        self.completed_items.append(item.to_db_record())

    def _audit_item(self, item):
        raise NotImplementedError

    def _audit_batch(self, batch):
        for item_dict in batch:
            self.increment('processed')
            try:
                item = MPUItem.from_db_record(item_dict)
                self.debug('auditing item %s', dict(item))
                if not item.deleted == 1:
                    self._audit_item(item)
                    self.increment('audited')
            except Exception as err:  # noqa
                self.exception('Error while auditing %s: %s',
                               item_dict['name'], str(err))
                self.increment('errors')
                # TODO: hmmm, should we revisit?

    def audit(self):
        self.broker.get_info()
        self.debug('visiting container')
        for batch in yield_item_batches(self.broker,
                                        self.config.max_batches,
                                        self.config.batch_size,
                                        include_states=self.audit_states,
                                        table=self.audit_table):
            self.completed_items = []
            self._audit_batch(batch)
            self.broker.merge_actions(self.completed_items)
        # TODO: run a TombstoneReclaimer on the action table
        self.info('%s', self._dump_stats())
        return None


class MpuOverwriteAuditor(BaseMpuAuditor):
    resource_type = 'object'

    def _cleanup_obsolete_item(self, item):
        # TODO: vary parts container according to policy OR put explicit path
        #  to parts in systags
        self.debug('cleaning up obsolete item %s', item.name)
        child_container = _safe_get_and_unquote(
            item.systags, 'child_container', self.broker.container)
        child_account = _safe_get_and_unquote(
            item.systags, 'child_account', self.broker.account)
        try:
            child = _safe_get_and_unquote(item.systags, 'child')
            if not child:
                raise ValueError('missing child in systags')
            self.client.delete_object(child_account, child_container, child)
            self.increment('marked')
        except Exception as err:  # noqa
            self.increment('errors')
            self.warning('Failed to delete child %s: %s', item, err)
        else:
            self._complete_item(item, self.ts_audit)

    def _item_has_version(self, item):
        if self.broker.container != self.user_container:
            self.debug('skip version check for %s', item.name)
            return False

        versions_container = build_versions_container_name(self.user_container)
        # TODO: we need to be more explicit about the expected version id
        obj_name, upload_id = item.name.split('\x00')
        version_id = UploadId.parse(upload_id).timestamp.internal
        version_name = build_versions_object_name(obj_name, version_id)

        try:
            self.client.get_object_metadata(
                self.user_account,
                versions_container,
                version_name,
                headers={'X-Newest': 'true'},
                acceptable_statuses=(404,)
            )
        except UnexpectedResponse as err:
            self.debug('version check for %s (%s/%s) returned %s',
                       item, versions_container, version_name,
                       err.resp.status)
            return True
        else:
            self.debug('version check for %s (%s/%s) returned 404',
                       item, versions_container, version_name)
            return False

    def _audit_item(self, item):
        if item.systags.get('child') and not self._item_has_version(item):
            return self._cleanup_obsolete_item(item)
        else:
            self.increment('noop')
            return None


class MpuPartMarkerAuditor(BaseMpuAuditor):
    resource_type = 'part'

    def _find_orphans(self, marker_item, prefix):
        # TODO: prefix query may scoop up some alien objects??
        #   need to check that each orphan is an MPU resource
        return [
            item for item in
            self._get_items_with_prefix(prefix, limit=1000, include_states={0})
            if item.name != marker_item.name]

    def _delete_resources(self, marker_item, prefix):
        orphan_parts = self._find_orphans(marker_item, prefix)
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

    def _process_marker(self, marker_item):
        try:
            child_prefix = _safe_get_and_unquote(
                marker_item.systags, 'child_prefix')
            if child_prefix:
                self._delete_resources(marker_item, child_prefix)
            else:
                self.increment('noop')
        except UnexpectedResponse:
            self.warning('failed to delete resources %s', marker_item.name)
        except Exception as err:  # noqa
            self.exception('Error deleting resources: %s', err)
        else:
            if marker_item.deleted != 1:
                try:
                    self._complete_item(marker_item, ts=self.ts_audit)
                except Exception as err:  # noqa
                    self.warning('Error deleting marker: %s', err)

    def _audit_item(self, item):
        self.debug('found marker %s %s', item.name, item.content_type)
        self.increment('marker')
        self._process_marker(item)


class MpuSessionAuditor(BaseMpuAuditor):
    resource_type = 'session'
    audit_table = 'object'

    def _is_mpu_in_user_namespace(self, obj_ref):
        """
        :raises UnexpectedResponse: if it is not possible to reliably confirm
            if the user object is a manifest for the given upload.
        :return: True if the user object is a manifest for the given upload,
            False if the user object is not a manifest for the given upload.
        """
        metadata = self.client.get_object_metadata(
            self.user_account, self.user_container, obj_ref.user_name,
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
            return

        if ctype_age > self.config.purge_delay:
            # time to clean-up everything
            try:
                lifeline_name = '%s/%s/' % (obj_ref.user_name, obj_ref.obj_id)
                self.debug('deleting aborted session %s, lifeline=%s',
                           session.name, lifeline_name)
                self.client.delete_object(
                    self.hidden_account, self.parts_container, lifeline_name)
                self._delete_session(session)
            except Exception as err:  # noqa
                self.warning('Failed to delete aborted session %s: %s',
                             session.name, err)
        else:
            # recheck later
            self.debug('aborted session deferred (age=%s)', ctype_age)

    def _audit_completed_session(self, session, obj_ref):
        try:
            self.debug('deleting completed session %s', session.name)
            self._delete_session(session)
        except Exception as err:  # noqa
            self.warning('Failed to prune completed session %s: %s',
                         session.name, err)

    def _audit_item(self, item):
        obj_ref = ObjectRef.parse(item.name)
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


class MpuAuditorDispatcher:
    """
    A plug-in for ContainerAuditor that dispatches audit work to a
    BaseMpuAuditor subclass according to the type of container
    """
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

    def _dispatch(self, auditor_class, broker, user_account, user_container):
        mpu_auditor = auditor_class(
            self.config, self.client, self.logger, broker,
            user_account, user_container)
        return mpu_auditor.audit()

    def audit(self, broker):
        auditor_class = None
        if is_mpu_hidden_account_name(broker.account):
            user_account = extract_user_account_name(broker.account)
            user_container, audit_type = parse_hidden_container_name(
                broker.container)
            if audit_type == 'mpu_parts':
                auditor_class = MpuPartMarkerAuditor
            elif audit_type == 'mpu_sessions':
                auditor_class = MpuSessionAuditor
        elif broker.container.endswith('+segments'):
            # TODO: I think this is redundant; maybe at one point the auditor
            #  was going to handle segments too???
            user_account = broker.account
            user_container = broker.container[:-1 * len('+segments')]
            auditor_class = MpuPartMarkerAuditor
        else:
            user_account = broker.account
            try:
                prefix, user_container = split_reserved_name(broker.container)
                if prefix == 'versions':
                    auditor_class = MpuOverwriteAuditor
            except ValueError:
                user_container = broker.container
                auditor_class = MpuOverwriteAuditor

        if auditor_class:
            self._dispatch(auditor_class, broker, user_account, user_container)
        else:
            self.logger.debug('unknown audit type for container %s, skipping',
                              broker.path)
