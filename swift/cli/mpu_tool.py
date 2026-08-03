# Copyright (c) 2026 NVIDIA
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

'''
Command-line tool for inspecting MPU (Multipart Upload) resources in a
Swift cluster.

Subcommands:

  info    List the resources associated with MPUs for a given account and
          container.
'''

import argparse
import io
import json
import os
import shutil
import sys
import tempfile
from collections import OrderedDict
from configparser import ConfigParser
from urllib.parse import quote

from swift.common.internal_client import InternalClient, UnexpectedResponse
from swift.common.middleware.mpu import (
    MPU_OBJECT_SYSMETA_PREFIX,
    MPU_SYSMETA_UPLOAD_ID_KEY,
    MPU_SESSION_CREATED_CONTENT_TYPE,
    MPU_SESSION_ABORTED_CONTENT_TYPE,
    MPU_SESSION_COMPLETING_CONTENT_TYPE,
    MPU_SESSION_COMPLETED_CONTENT_TYPE,
    make_mpu_hidden_account_name,
    make_parts_container_name,
    make_sessions_container_name,
)
from swift.common.object_ref import ObjectRef
from swift.common.utils import readconf

PART_SIZE = 5 * 1024 * 1024

# request backend containers whose names contain reserved (NUL) bytes
RESERVED_NAMES_HEADER = {'X-Backend-Allow-Reserved-Names': 'true'}
# request that mpu sysmeta (including the internal upload-id) is returned
# on HEAD/GET responses for manifest objects
INCLUDE_MPU_SYSMETA_HEADER = {'X-Backend-Include-Mpu-Sysmeta': 'true'}

SESSION_STATES = {
    MPU_SESSION_CREATED_CONTENT_TYPE: 'created',
    MPU_SESSION_COMPLETING_CONTENT_TYPE: 'completing',
    MPU_SESSION_COMPLETED_CONTENT_TYPE: 'completed',
    MPU_SESSION_ABORTED_CONTENT_TYPE: 'aborted',
}


def _make_internal_client(conf_path, user_agent, request_tries):
    """
    Create an InternalClient with the mpu (and slo) middleware in its
    pipeline, so that MPU API requests are handled correctly.

    Reads the config at conf_path, inserts filter:slo and filter:mpu into
    the pipeline if not already present, writes the modified config to a
    temporary file, and returns an InternalClient built from it.
    """
    def _insert(pipeline, name, after):
        if name in pipeline:
            return
        index = 0
        for other in after:
            if other in pipeline:
                index = max(index, pipeline.index(other) + 1)
        pipeline.insert(index, name)

    _after = ['copy', 'staticweb', 'tempauth', 'keystoneauth',
              'catch_errors', 'gatekeeper', 'cache', 'proxy-logging']

    conf = readconf(conf_path)
    conf.pop('log_name', None)
    conf.pop('__file__', None)
    pipeline = conf['pipeline:main']['pipeline'].split()

    conf.setdefault('filter:mpu', {'use': 'egg:swift#mpu'})
    _insert(pipeline, 'mpu', _after)

    conf['pipeline:main']['pipeline'] = ' '.join(pipeline)

    tmpdir = tempfile.mkdtemp()
    try:
        tmp_path = os.path.join(tmpdir, 'internal-client.conf')
        cp = ConfigParser()
        cp.read_dict(conf)
        with open(tmp_path, 'w') as fd:
            cp.write(fd)
        return InternalClient(
            tmp_path, user_agent, request_tries,
            global_conf={'log_name': 'mpu-tool-ic'})
    finally:
        shutil.rmtree(tmpdir)


def object_listing_range(obj):
    """
    Return ``(marker, end_marker)`` that bound a parts or sessions listing to
    just the entries for a single user object.

    Entries are named ``<obj>/<upload_id>[/<tail>]``; ``end_marker`` replaces
    the trailing ``/`` (0x2f) with the next code point (0x30) so it sorts
    immediately after all of the object's entries.
    """
    return obj + '/', obj + chr(ord('/') + 1)


def _object_name(item):
    # the user object name is common to all the sessions and parts of a given
    # object; fall back to the raw internal name if it cannot be parsed
    try:
        return ObjectRef.parse(item['name']).user_name
    except ValueError:
        return item.get('name')


def _upload_id(item):
    # the upload id is common to all the parts and the session of a single
    # upload of an object; fall back to None if it cannot be parsed
    try:
        return ObjectRef.parse(item['name']).obj_id
    except ValueError:
        return None


def _session_record(item, full):
    if full:
        return dict(item)
    return {
        'state': SESSION_STATES.get(
            item.get('content_type'), item.get('content_type')),
    }


def _part_record(item, full):
    if full:
        return dict(item)
    ref = ObjectRef.parse(item['name'])
    return {
        'part_number': ref.tail,
        'size': item.get('bytes'),
    }


def _safe_record(transform, item, full):
    try:
        return transform(item, full)
    except ValueError:
        # an unexpected/malformed internal name; fall back to the raw name
        # rather than aborting the whole listing
        return {'name': item.get('name')}


def group_by_object(part_items, session_items, full, limit):
    """
    Group parts and sessions by user object name and then by upload id.

    Object names are discovered from the parts listing first (preserving
    listing order); each object's sessions are then attached. Objects that
    appear only in the sessions listing are appended after those found from
    parts. Within each object, parts and sessions are grouped by upload id
    (there may be more than one concurrent upload of the same object).

    At most ``limit`` distinct objects are collected: once that many have been
    seen, iteration of the (paginated) listings is stopped so that no further
    pages are fetched.

    :returns: a list of ``{'object', 'uploads': [{'upload_id', 'parts',
        'sessions'}]}`` dicts.
    """
    objects = OrderedDict()

    def get_upload(name, upload_id):
        if name not in objects:
            objects[name] = {'object': name, 'uploads': OrderedDict()}
        uploads = objects[name]['uploads']
        if upload_id not in uploads:
            uploads[upload_id] = {
                'upload_id': upload_id, 'parts': [], 'sessions': []}
        return uploads[upload_id]

    for item in part_items:
        name = _object_name(item)
        if name not in objects and len(objects) >= limit:
            break
        get_upload(name, _upload_id(item))['parts'].append(
            _safe_record(_part_record, item, full))
    for item in session_items:
        name = _object_name(item)
        if name not in objects and len(objects) >= limit:
            break
        get_upload(name, _upload_id(item))['sessions'].append(
            _safe_record(_session_record, item, full))

    # flatten each object's uploads mapping into a list
    result = []
    for entry in objects.values():
        entry['uploads'] = list(entry['uploads'].values())
        result.append(entry)
    return result


def _manifest_record(metadata, full):
    if full:
        return dict(metadata)
    # user-facing subset: the mpu sysmeta with its prefix stripped
    return {key[len(MPU_OBJECT_SYSMETA_PREFIX):]: value
            for key, value in metadata.items()
            if key.startswith(MPU_OBJECT_SYSMETA_PREFIX)}


def add_manifests(swift, account, container, objects, full):
    """
    For each grouped object, HEAD the object in the user container and, if it
    exists, attach its metadata as a ``manifest`` item on the upload group
    identified by the manifest's upload id. If no matching upload group is
    present (its parts and session are already gone) a new upload group is
    appended to hold the manifest.
    """
    for entry in objects:
        try:
            metadata = swift.get_object_metadata(
                account, container, entry['object'],
                headers=INCLUDE_MPU_SYSMETA_HEADER)
        except UnexpectedResponse:
            # no manifest object exists (or it is not accessible); leave the
            # object without a manifest item
            continue
        upload_id = metadata.get(MPU_SYSMETA_UPLOAD_ID_KEY)
        group = None
        for upload in entry['uploads']:
            if upload['upload_id'] == upload_id:
                group = upload
                break
        if group is None:
            group = {'upload_id': upload_id, 'parts': [], 'sessions': []}
            entry['uploads'].append(group)
        group['manifest'] = _manifest_record(metadata, full)


def _print_records(records, indent):
    for record in records:
        print(indent + '  '.join(
            '%s=%s' % (key, value) for key, value in record.items()))


def _print_summary(result):
    print('user_container: %s' % result['user_container'])
    print('parts_container: %s' % result['parts_container'])
    print('sessions_container: %s' % result['sessions_container'])
    print()
    for entry in result['objects']:
        print('%s:' % entry['object'])
        for upload in entry['uploads']:
            print('  upload_id=%s:' % upload['upload_id'])
            # show only part counts, not every part: the count of parts that
            # have a part number suffix plus the count of those that do not
            # (e.g. the lifeline object)
            with_suffix = sum(1 for part in upload['parts']
                              if part.get('part_number'))
            without_suffix = len(upload['parts']) - with_suffix
            print('    Parts: %d + %d' % (with_suffix, without_suffix))
            print('    Sessions (%d):' % len(upload['sessions']))
            _print_records(upload['sessions'], '      ')
            if 'manifest' in upload:
                print('    Manifest:')
                _print_records([upload['manifest']], '      ')


def _add_make_subparser(subparsers):
    '''
    Create a multipart upload of a synthetic object using the MPU API.

    Three API calls are made in sequence:

      1. POST ?uploads to create an upload session and obtain an upload-id.
         If the container does not yet exist it is created automatically.
      2. PUT ?upload-id=<id>&part-number=1 to upload a single 5 MB part
         whose body is the ASCII string 'test' repeated to fill the part.
      3. POST ?upload-id=<id> with a JSON manifest body to complete the
         upload.

    The upload-id and per-step results are printed to stdout.
    '''
    p = subparsers.add_parser(
        'make',
        description=_add_make_subparser.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help='Create a test MPU object.')
    p.add_argument('account', help='User account name.')
    p.add_argument('container', help='User container name.')
    p.add_argument('object', help='Object name.')
    p.add_argument(
        '--config', default='/etc/swift/internal-client.conf',
        help='Path to the internal client config file '
             '(default: /etc/swift/internal-client.conf).')
    p.add_argument(
        '--request-tries', type=int, default=3,
        help='Number of times to try each backend request (default: 3).')
    return p


def _run_make(args):
    try:
        swift = _make_internal_client(
            args.config, 'Swift MPU Tool', args.request_tries)
    except (OSError, IOError) as err:
        sys.exit('Error loading internal client from %s: %s'
                 % (args.config, err))

    obj_path = '/v1/%s/%s/%s' % (
        quote(args.account, safe=''),
        quote(args.container, safe=''),
        quote(args.object, safe=''))
    cont_path = '/v1/%s/%s' % (
        quote(args.account, safe=''),
        quote(args.container, safe=''))

    # Step 1: create upload session; on 404 create the container and retry
    print('Creating MPU session for %s/%s/%s ...'
          % (args.account, args.container, args.object))
    try:
        resp = swift.make_request(
            'POST', obj_path, {}, (2, 404), params={'uploads': ''})
    except UnexpectedResponse as err:
        sys.exit('Error creating MPU session: %s' % err)
    if resp.status_int == 404:
        print('  container not found; creating %s/%s ...'
              % (args.account, args.container))
        try:
            swift.make_request('PUT', cont_path, {}, (2,))
        except UnexpectedResponse as err:
            sys.exit('Error creating container: %s' % err)
        try:
            resp = swift.make_request(
                'POST', obj_path, {}, (2,), params={'uploads': ''})
        except UnexpectedResponse as err:
            sys.exit('Error creating MPU session: %s' % err)
    upload_id = resp.headers.get('X-Upload-Id')
    if not upload_id:
        sys.exit('Error: server did not return X-Upload-Id')
    print('  upload_id: %s' % upload_id)

    # Step 2: upload a single part whose body is 'test' repeated to 5 MB
    part_body = b'test' * (PART_SIZE // 4)
    print('Uploading part 1 (%d bytes) ...' % len(part_body))
    try:
        resp = swift.make_request(
            'PUT', obj_path,
            {'Content-Length': str(len(part_body)),
             'Content-Type': 'application/octet-stream'},
            (2,),
            body_file=io.BytesIO(part_body),
            params={'upload-id': upload_id, 'part-number': '1'})
    except UnexpectedResponse as err:
        sys.exit('Error uploading part: %s' % err)
    etag = resp.headers.get('Etag', '').strip('"')
    if not etag:
        sys.exit('Error: server did not return an Etag for the uploaded part')
    print('  etag: %s' % etag)

    # Step 3: complete the upload with a single-part manifest
    manifest_body = json.dumps(
        [{'part_number': 1, 'etag': etag}]).encode('ascii')
    print('Completing upload ...')
    try:
        resp = swift.make_request(
            'POST', obj_path,
            {'Content-Length': str(len(manifest_body)),
             'Content-Type': 'application/json'},
            (2,),
            body_file=io.BytesIO(manifest_body),
            params={'upload-id': upload_id})
    except UnexpectedResponse as err:
        sys.exit('Error completing upload: %s' % err)

    # complete_upload returns a streaming heartbeat body; the JSON result
    # follows the last \r\n\r\n separator in the response body
    body = resp.body
    sep = b'\r\n\r\n'
    idx = body.rfind(sep)
    try:
        result = json.loads(body[idx + len(sep):] if idx >= 0 else body)
    except ValueError:
        sys.exit('Error: could not parse complete response: %r' % body[:200])

    status = result.get('Response Status', '')
    if status.startswith('2'):
        print('  status: %s' % status)
        if 'Etag' in result:
            print('  etag: %s' % result['Etag'])
        return 0
    sys.exit('Error: complete upload failed: %s\n%s' % (
        status, result.get('Response Body', '')))


def _add_list_subparser(subparsers):
    p = subparsers.add_parser(
        'list',
        help='List a hidden MPU container.',
        description='List the contents of a hidden MPU container, one item '
                    'per line showing name and content-type.')
    p.add_argument('account', help='User account name.')
    p.add_argument('container', help='User container name.')
    p.add_argument('resource', choices=['parts', 'sessions'],
                   help='Which hidden container to list.')
    p.add_argument(
        '--config', default='/etc/swift/internal-client.conf',
        help='Path to the internal client config file '
             '(default: /etc/swift/internal-client.conf).')
    p.add_argument(
        '--request-tries', type=int, default=3,
        help='Number of times to try each backend request (default: 3).')
    return p


def _run_list(args):
    hidden_account = make_mpu_hidden_account_name(args.account)
    if args.resource == 'parts':
        hidden_container = make_parts_container_name(args.container)
    else:
        hidden_container = make_sessions_container_name(args.container)

    try:
        swift = _make_internal_client(
            args.config, 'Swift MPU Tool', args.request_tries)
    except (OSError, IOError) as err:
        sys.exit('Error loading internal client from %s: %s'
                 % (args.config, err))

    try:
        for item in swift.iter_objects(
                hidden_account, hidden_container,
                headers=RESERVED_NAMES_HEADER):
            print('%s\t%s' % (item['name'], item.get('content_type', '')))
    except UnexpectedResponse as err:
        sys.exit('Error listing %s: %s' % (args.resource, err))
    return 0


def _add_info_subparser(subparsers):
    '''
    List the resources associated with Multipart Uploads (MPUs) for a given
    account and container.

    MPU state is stored in hidden reserved containers in a hidden reserved
    account: an ``mpu_sessions`` container tracks in-progress upload sessions
    and an ``mpu_parts`` container holds the uploaded parts.

    Results are grouped by user object name and then by upload id (a single
    object may have more than one concurrent upload). Object names are
    discovered first from the parts listing (preserving listing order), with
    each object's sessions then attached; objects that appear only in the
    sessions listing (i.e. have no parts) are appended after those found from
    parts. For each object a HEAD is made to the object in the user container;
    if it exists, its metadata is attached as a ``manifest`` item on the
    matching upload id.

    An optional ``object`` argument restricts the search to a single object
    name. The number of objects searched is capped by ``--limit`` (default 10).

    By default the raw listing fields as returned by the container server are
    emitted as JSON. Pass ``--summary`` for plain text output showing only
    user-facing names (the internal upload-id and, for parts, the part number);
    in summary mode only part counts are shown for each object, not every part,
    as ``<parts with a part number suffix> + <parts without a suffix>`` (a part
    with no suffix is the lifeline object).

    Note: the upload-id shown is Swift's internal upload-id. It is not the
    same as the externally issued upload-id returned to API clients, which is
    derived using a signing key held only by the mpu middleware.
    '''
    p = subparsers.add_parser(
        'info',
        description=_add_info_subparser.__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help='List MPU resources for an account and container.')
    p.add_argument('account', help='User account name.')
    p.add_argument('container', help='User container name.')
    p.add_argument(
        'object', nargs='?', default=None,
        help='Restrict the search to a single user object name.')
    p.add_argument(
        '--config', default='/etc/swift/internal-client.conf',
        help='Path to the internal client config file '
             '(default: /etc/swift/internal-client.conf).')
    p.add_argument(
        '--request-tries', type=int, default=3,
        help='Number of times to try each backend request (default: 3).')
    p.add_argument(
        '--limit', type=int, default=10,
        help='Maximum number of objects to search (default: 10).')
    p.add_argument(
        '--summary', action='store_true',
        help='Emit plain text showing only user-facing names, instead of '
             'the default JSON with raw listing fields.')
    return p


def _run_info(args):
    hidden_account = make_mpu_hidden_account_name(args.account)
    sessions_container = make_sessions_container_name(args.container)
    parts_container = make_parts_container_name(args.container)

    try:
        swift = _make_internal_client(
            args.config, 'Swift MPU Tool', args.request_tries)
    except (OSError, IOError) as err:
        sys.exit('Error loading internal client from %s: %s'
                 % (args.config, err))

    # default (not --summary): raw listing fields, emitted as JSON
    full = not args.summary
    if args.object:
        marker, end_marker = object_listing_range(args.object)
    else:
        marker, end_marker = '', ''

    # iter_objects returns paginated generators; group_by_object stops
    # iterating (and so stops fetching pages) once the object limit is reached
    part_items = swift.iter_objects(
        hidden_account, parts_container, marker=marker, end_marker=end_marker,
        headers=RESERVED_NAMES_HEADER)
    session_items = swift.iter_objects(
        hidden_account, sessions_container, marker=marker,
        end_marker=end_marker, headers=RESERVED_NAMES_HEADER)
    try:
        objects = group_by_object(
            part_items, session_items, full, args.limit)
    except UnexpectedResponse as err:
        sys.exit('Error listing MPU resources: %s' % err)

    add_manifests(swift, args.account, args.container, objects, full)

    # show each container as '<account>/<container>' (the parts and sessions
    # containers live in the hidden account); the reserved container names
    # contain NUL bytes so url-quote the paths for safe, readable output
    result = {
        'user_container': quote('%s/%s' % (args.account, args.container)),
        'parts_container': quote('%s/%s' % (hidden_account, parts_container)),
        'sessions_container': quote(
            '%s/%s' % (hidden_account, sessions_container)),
        'objects': objects,
    }
    if args.summary:
        _print_summary(result)
    else:
        print(json.dumps(result, indent=2))
    return 0


def main(args=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='subcommand')
    _add_make_subparser(subparsers)
    _add_list_subparser(subparsers)
    _add_info_subparser(subparsers)

    parsed = parser.parse_args(args)
    if parsed.subcommand == 'make':
        return _run_make(parsed)
    if parsed.subcommand == 'list':
        return _run_list(parsed)
    if parsed.subcommand == 'info':
        return _run_info(parsed)
    parser.print_help()
    return 1


if __name__ == '__main__':
    main()
