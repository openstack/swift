# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

'''
Enqueue background jobs to delete portions of a container's namespace.

Accepts prefix, marker, and end-marker args that work as in container
listings. Objects found in the listing will be marked to be deleted
by the object-expirer; until the object is actually deleted, it will
continue to appear in listings.

If there are many objects, this operation may take some time. Stats will
periodically be emitted so you know the process hasn't hung. These will
also include the last object marked for deletion; if there is a failure,
pass this as the ``--marker`` when retrying to minimize duplicative work.
'''

import argparse
import io
import itertools
import json
import time

from swift.common.internal_client import InternalClient
from swift.common.utils import Timestamp, MD5_OF_EMPTY_STRING
from swift.obj.expirer import build_task_obj, ASYNC_DELETE_TYPE

OBJECTS_PER_UPDATE = 10000


def make_delete_jobs(account, container, objects, timestamp):
    '''
    Create a list of async-delete jobs

    :param account: (native or unicode string) account to delete from
    :param container: (native or unicode string) container to delete from
    :param objects: (list of native or unicode strings) objects to delete
    :param timestamp: (Timestamp) time at which objects should be marked
                      deleted
    :returns: list of dicts appropriate for an UPDATE request to an
              expiring-object queue
    '''
    return [
        {
            'name': build_task_obj(
                timestamp, account, container,
                obj, high_precision=True),
            'deleted': 0,
            'created_at': timestamp.internal,
            'etag': MD5_OF_EMPTY_STRING,
            'size': 0,
            'storage_policy_index': 0,
            'content_type': ASYNC_DELETE_TYPE,
        } for obj in objects]


def mark_for_deletion(swift, account, container, marker, end_marker,
                      prefix, timestamp=None, yield_time=10):
    '''
    Enqueue jobs to async-delete some portion of a container's namespace

    :param swift: InternalClient to use
    :param account: account to delete from
    :param container: container to delete from
    :param marker: only delete objects after this name
    :param end_marker: only delete objects before this name. Use ``None`` or
                       empty string to delete to the end of the namespace.
    :param prefix: only delete objects starting with this prefix
    :param timestamp: delete all objects as of this time. If ``None``, the
                      current time will be used.
    :param yield_time: approximate period with which intermediate results
                       should be returned. If ``None``, disable intermediate
                       results.
    :returns: If ``yield_time`` is ``None``, the number of objects marked for
              deletion. Otherwise, a generator that will yield out tuples of
              ``(number of marked objects, last object name)`` approximately
              every ``yield_time`` seconds. The final tuple will have ``None``
              as the second element. This form allows you to retry when an
              error occurs partway through while minimizing duplicate work.
    '''
    if timestamp is None:
        timestamp = Timestamp.now()

    def enqueue_deletes():
        deleted = 0
        obj_iter = swift.iter_objects(
            account, container,
            marker=marker, end_marker=end_marker, prefix=prefix)
        time_marker = time.time()
        while True:
            to_delete = [obj['name'] for obj in itertools.islice(
                obj_iter, OBJECTS_PER_UPDATE)]
            if not to_delete:
                break
            delete_jobs = make_delete_jobs(
                account, container, to_delete, timestamp)
            swift.make_request(
                'UPDATE',
                swift.make_path('.expiring_objects', str(int(timestamp))),
                headers={'X-Backend-Allow-Private-Methods': 'True',
                         'X-Backend-Storage-Policy-Index': '0',
                         'X-Timestamp': timestamp.internal},
                acceptable_statuses=(2,),
                body_file=io.BytesIO(json.dumps(delete_jobs).encode('ascii')))
            deleted += len(delete_jobs)
            if yield_time is not None and \
                    time.time() - time_marker > yield_time:
                yield deleted, to_delete[-1]
                time_marker = time.time()
        yield deleted, None

    if yield_time is None:
        for deleted, marker in enqueue_deletes():
            if marker is None:
                return deleted
    else:
        return enqueue_deletes()


def main(args=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--config', default='/etc/swift/internal-client.conf',
                        help=('internal-client config file '
                              '(default: /etc/swift/internal-client.conf'))
    parser.add_argument('--request-tries', type=int, default=3,
                        help='(default: 3)')
    parser.add_argument('account', help='account from which to delete')
    parser.add_argument('container', help='container from which to delete')
    parser.add_argument(
        '--prefix', default='',
        help='only delete objects with this prefix (default: none)')
    parser.add_argument(
        '--marker', default='',
        help='only delete objects after this marker (default: none)')
    parser.add_argument(
        '--end-marker', default='',
        help='only delete objects before this end-marker (default: none)')
    parser.add_argument(
        '--timestamp', type=Timestamp, default=Timestamp.now(),
        help='delete all objects as of this time (default: now)')
    args = parser.parse_args(args)

    swift = InternalClient(
        args.config, 'Swift Container Deleter', args.request_tries,
        global_conf={'log_name': 'container-deleter-ic'})
    for deleted, marker in mark_for_deletion(
            swift, args.account, args.container,
            args.marker, args.end_marker, args.prefix, args.timestamp):
        if marker is None:
            print('Finished. Marked %d objects for deletion.' % deleted)
        else:
            print('Marked %d objects for deletion, through %r' % (
                deleted, marker))


if __name__ == '__main__':
    main()
