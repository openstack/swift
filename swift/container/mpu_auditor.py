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

from swift.common.internal_client import InternalClient
from swift.common.middleware.mpu import MPU_DELETED_CONTENT_TYPE
from swift.common.request_helpers import split_reserved_name
from swift.common.utils import Timestamp, get_logger
from swift.container.backend import ContainerBroker


# TODO: share with object_versioning?  move to request_helpers?
def safe_split_reserved_name(reserved_name):
    # TODO: this assumes 2 parts, should it be generalised
    try:
        return split_reserved_name(reserved_name)
    except ValueError:
        return None, reserved_name


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
    parts = name.strip('/').rsplit('/', 2)
    return '/'.join(parts[:2])


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


class BaseMpuBrokerAuditor(object):
    ROWS_PER_BATCH = 1000

    def __init__(self, client, logger, broker):
        self.client = client
        self.logger = logger
        self.broker = broker
        self.checked = {}
        self.keep = {}

    def log(self, log_func, msg):
        data = {
            'msg': msg,
            'db_file': self.broker.db_file,
            'path': self.broker.path,
        }
        log_func('mpu_auditor ' + json.dumps(data))

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

    def _get_item(self, name, include_deleted=False):
        self.debug('get_item %s', name)
        items = self._get_items_with_prefix(
            name, limit=1, include_deleted=include_deleted)
        if items:
            return items[0]
        return None

    def _find_orphans(self, marker, upload):
        # TODO: prefix query may scoop up some alien objects??
        #   need to check that each orphan is an MPU manifest
        return [
            item for item in
            self._get_items_with_prefix(upload, limit=1000)
            if item.name != marker.name]

    def _process_marker(self, marker, upload):
        # we only expect one manifest per marker, but w/e
        # TODO: do we even need to find the manifest row? could just send a
        #  DELETE and see how SLO responds?
        orphans = self._find_orphans(marker, upload)
        self.checked[upload] = orphans

        for orphan in orphans:
            self._delete_orphan(orphan)

        if not marker.deleted:
            # TODO: do we do this even if we didn't find any manifest row yet?
            # Delete the marker now so that it will eventually be reclaimed. We
            # can still find the marker row for subsequent checks until it is
            # reclaimed.
            ts = Timestamp(marker.timestamp, offset=1)
            self.client.delete_object(
                self.broker.account, self.broker.container, marker.name,
                headers={'X-Timestamp': ts.internal})

    def _audit_item(self, item):
        if item.deleted:
            return
        try:
            upload = extract_upload_prefix(item.name)
        except ValueError as err:
            self.log(self.log.warning, 'mpu_audit %s' % err)
            return

        if upload in self.checked:
            return

        if item.content_type == MPU_DELETED_CONTENT_TYPE:
            self.debug('found_marker %s', item.name)
            self._process_marker(item, upload)
        else:
            self.debug('found_other %s', item.name)
            marker_name = upload + '/deleted'
            marker_item = self._get_item(marker_name, include_deleted=None)
            if marker_item:
                self.debug('found_other_marker %s', marker_item.name)
                self._process_marker(marker_item, upload)

    def audit(self):
        self.broker.get_info()
        self.debug('mpu_audit visiting %s', self.broker.path)

        context = MpuAuditorContext.load(self.broker)
        self.debug('auditing from row %s' % context.last_audit_row)
        items = [
            (it['ROWID'], Item(**it))
            for it in self.broker.get_items_since(
                context.last_audit_row, self.ROWS_PER_BATCH)
        ]
        for row_id, item in items:
            context.last_audit_row = row_id
            self._audit_item(item)
        context.store(self.broker)
        self.debug('processed_rows %d', len(items))
        return None


class MpuBrokerAuditor(BaseMpuBrokerAuditor):
    def _delete_orphan_parts(self, orphan_manifest):
        # TODO: add error handling!
        # there is no SLO in internal client so we have to fetch the manifest
        # and delete the parts here
        status, hdrs, resp_iter = self.client.get_object(
            self.broker.account, self.broker.container, orphan_manifest.name,
            params={'multipart-manifest': 'get'})
        # TODO: be defensive - check it is a manifest!
        manifest = json.loads(b''.join(resp_iter))
        for item in manifest:
            self.debug('deleting part %s', item['name'])
            container, obj = item['name'].lstrip('/').split('/', 1)
            self.client.delete_object(
                self.broker.account, container, obj)

    def _delete_orphan(self, orphan):
        # TODO: handle failed requests
        self._delete_orphan_parts(orphan)
        self.debug('deleting manifest %s', orphan.name)
        self.client.delete_object(
            self.broker.account, self.broker.container, orphan.name)


class SloMpuBrokerAuditor(BaseMpuBrokerAuditor):
    def _delete_orphan(self, orphan):
        self.debug('deleting segment %s', orphan.name)
        self.client.delete_object(self.broker.account,
                                  self.broker.container,
                                  orphan.name)


class MpuAuditor(object):
    def __init__(self, conf, logger=None):
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
        qualifier, container = safe_split_reserved_name(broker.container)
        self.logger.debug('mpu_audit visiting %s %s', qualifier, container)
        if qualifier == 'mpu_manifests':
            mpu_auditor = MpuBrokerAuditor(self.client, self.logger, broker)
            return mpu_auditor.audit()
        elif broker.path.endswith('+segments'):
            mpu_auditor = SloMpuBrokerAuditor(self.client, self.logger, broker)
            return mpu_auditor.audit()
        else:
            return None
