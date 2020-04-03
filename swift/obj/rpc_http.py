# Copyright (c) 2010-2012 OpenStack Foundation
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

# Implements RPC: protobuf over a UNIX domain socket

import socket
from eventlet.green import httplib
from swift.obj import fmgr_pb2 as pb


class UnixHTTPConnection(httplib.HTTPConnection):
    """Support for unix domain socket with httplib"""
    def __init__(self, path, host='localhost', port=None, strict=None,
                 timeout=None):
        httplib.HTTPConnection.__init__(self, host, port=port, strict=strict,
                                        timeout=timeout)
        self.path = path

    def connect(self):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(self.path)
        self.sock = sock


class StatusCode(object):
    Ok = 200
    Cancelled = 299
    InvalidArgument = 400
    NotFound = 404
    AlreadyExists = 409
    PermissionDenied = 403
    FailedPrecondition = 412
    Unimplemented = 501
    Internal = 500
    Unavailable = 503


class RpcError(Exception):
    def __init__(self, message, code):
        self.code = code
        super(RpcError, self).__init__(message)


def get_rpc_reply(conn, pb_type):
    """
    Read the response from the index server over HTTP. If the status is 200,
    deserialize the body as a protobuf object and return it.
    If the status is not 200, raise an RpcError exception.
    :param conn: HTTP connection to the index server
    :param pb_type: protobuf type we expect in return
    :return: protobuf object, or raise an exception if HTTP status is not 200
    """
    # if buffering is not set, httplib will call recvfrom() for every char
    http_response = conn.getresponse(buffering=True)
    if http_response.status != StatusCode.Ok:
        raise RpcError(http_response.read(), http_response.status)

    pb_obj = pb_type()
    pb_obj.ParseFromString(http_response.read())
    return pb_obj


def get_next_offset(socket_path, volume_index, repair_tool=False):
    """
    Returns the next offset to use in the volume
    """
    volume = pb.GetNextOffsetInfo(volume_index=int(volume_index),
                                  repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/get_next_offset', volume.SerializeToString())
    response = get_rpc_reply(conn, pb.VolumeNextOffset)
    return response.offset


def register_volume(socket_path, partition, volume_type, volume_index,
                    first_obj_offset, state, repair_tool=False):
    volume = pb.NewVolumeInfo(partition=int(partition), type=int(volume_type),
                              volume_index=int(volume_index),
                              offset=first_obj_offset, state=state,
                              repair_tool=repair_tool)

    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/register_volume', volume.SerializeToString())
    response = get_rpc_reply(conn, pb.NewVolumeReply)
    return response


def unregister_volume(socket_path, volume_index):
    index = pb.VolumeIndex(index=volume_index)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/unregister_volume', index.SerializeToString())
    response = get_rpc_reply(conn, pb.Empty)
    return response


def update_volume_state(socket_path, volume_index, new_state,
                        repair_tool=False):
    state_update = pb.NewVolumeState(volume_index=int(volume_index),
                                     state=new_state, repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/update_volume_state',
                 state_update.SerializeToString())
    response = get_rpc_reply(conn, pb.Empty)
    return response


def register_object(socket_path, name, volume_index, offset, next_offset,
                    repair_tool=False):
    """
    register a vfile
    """
    obj = pb.NewObjectInfo(name=str(name), volume_index=int(volume_index),
                           offset=int(offset),
                           next_offset=int(next_offset),
                           repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/register_object', obj.SerializeToString())
    response = get_rpc_reply(conn, pb.NewObjectReply)
    return response


def unregister_object(socket_path, name):
    obj = pb.ObjectName(name=str(name))
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/unregister_object', obj.SerializeToString())
    response = get_rpc_reply(conn, pb.DelObjectReply)
    return response


def rename_object(socket_path, name, new_name, repair_tool=False):
    rename_info = pb.RenameInfo(name=str(name), new_name=str(new_name),
                                repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/rename_object', rename_info.SerializeToString())
    response = get_rpc_reply(conn, pb.RenameReply)
    return response


def quarantine_object(socket_path, name, repair_tool=False):
    objname = pb.ObjectName(name=str(name), repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/quarantine_object', objname.SerializeToString())
    response = get_rpc_reply(conn, pb.Empty)
    return response


def unquarantine_object(socket_path, name, repair_tool=False):
    objname = pb.ObjectName(name=str(name), repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/unquarantine_object', objname.SerializeToString())
    response = get_rpc_reply(conn, pb.Empty)
    return response


def _list_quarantined_ohashes(socket_path, page_token, page_size):
    """
    Returns quarantined hashes, with pagination (as with the regular diskfile,
    they are not below partition/suffix directories)
    :param socket_path: socket_path for index-server
    :param page_token: where to start for pagination
    :param page_size: maximum number of results to be returned
    :return: A list of quarantined object hashes
    """
    req_args = pb.ListQuarantinedOHashesInfo(page_token=str(page_token),
                                             page_size=page_size)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/list_quarantined_ohashes',
                 req_args.SerializeToString())
    response = get_rpc_reply(conn, pb.QuarantinedObjectNames)
    return response


def list_quarantined_ohashes(socket_path, page_size=10000):
    """
    Returns all quarantined hashes, wraps _list_quarantined_ohashes so caller
    does not have to deal with pagination
    :param socket_path: socket_path
    :param page_size: page_size to pass to wrapped function
    :return: an iterator for all quarantined objects
    """
    page_token = ""
    while True:
        response = _list_quarantined_ohashes(socket_path, page_token,
                                             page_size)
        for r in response.objects:
            yield(r)
        page_token = response.next_page_token
        if not page_token:
            break


def _list_objects_by_volume(socket_path, volume_index, quarantined, page_token,
                            page_size, repair_tool=False):
    """
    Returns objects within the volume, either quarantined or not, with
    pagination.
    :param socket_path: socket_path for index-server
    :param volume_index: index of the volume for which to list objects
    :param quarantined: if true, returns quarantined objects. if false, returns
    non-quarantined objects.
    :param page_token: where to start for pagination
    :param page_size: maximum number of results to be returned
    :param repair_tool: set to true if caller is a repair tool
    :return: A list of objects for the volume
    """
    req_args = pb.VolumeIndex(index=volume_index, quarantined=quarantined,
                              page_token=page_token, page_size=page_size,
                              repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/load_objects_by_volume',
                 req_args.SerializeToString())
    response = get_rpc_reply(conn, pb.LoadObjectsResponse)
    return response


def list_objects_by_volume(socket_path, volume_index, quarantined=False,
                           page_size=10000, repair_tool=False):
    page_token = ""
    while True:
        response = _list_objects_by_volume(socket_path, volume_index,
                                           quarantined, page_token, page_size,
                                           repair_tool)
        for r in response.objects:
            yield(r)
        page_token = response.next_page_token
        if not page_token:
            break


def list_quarantined_ohash(socket_path, prefix, repair_tool=False):
    len_prefix = len(prefix)
    prefix = pb.ObjectPrefix(prefix=str(prefix), repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/list_quarantined_ohash', prefix.SerializeToString())
    response = get_rpc_reply(conn, pb.LoadObjectsResponse)

    # Caller expects object names without the prefix, similar
    # to os.listdir, not actual objects.
    objnames = []
    for obj in response.objects:
        objnames.append(obj.name[len_prefix:])

    return objnames


# listdir like function for the KV
def list_prefix(socket_path, prefix, repair_tool=False):
    len_prefix = len(prefix)
    prefix = str(prefix)
    prefix = pb.ObjectPrefix(prefix=prefix, repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/load_objects_by_prefix', prefix.SerializeToString())
    response = get_rpc_reply(conn, pb.LoadObjectsResponse)
    # response.objets is an iterable
    # TBD, caller expects object names without the prefix, similar
    # to os.listdir, not actual objects.
    # Fix this in the rpc server
    # return response.objects
    objnames = []
    for obj in response.objects:
        objnames.append(obj.name[len_prefix:])

    return objnames


def get_object(socket_path, name, is_quarantined=False, repair_tool=False):
    """
    returns an object given its whole key
    """
    object_name = pb.LoadObjectInfo(name=str(name),
                                    is_quarantined=is_quarantined,
                                    repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/load_object', object_name.SerializeToString())
    response = get_rpc_reply(conn, pb.Object)
    return response


def list_partitions(socket_path, partition_bits):
    list_partitions_info = pb.ListPartitionsInfo(
        partition_bits=partition_bits)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/list_partitions',
                 list_partitions_info.SerializeToString())
    response = get_rpc_reply(conn, pb.DirEntries)
    return response.entry


def list_partition(socket_path, partition, partition_bits):
    list_partition_info = pb.ListPartitionInfo(partition=partition,
                                               partition_bits=partition_bits)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/list_partition',
                 list_partition_info.SerializeToString())
    response = get_rpc_reply(conn, pb.DirEntries)
    return response.entry


def list_suffix(socket_path, partition, suffix, partition_bits):
    suffix = str(suffix)
    list_suffix_info = pb.ListSuffixInfo(partition=partition,
                                         suffix=suffix,
                                         partition_bits=partition_bits)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/list_suffix', list_suffix_info.SerializeToString())
    response = get_rpc_reply(conn, pb.DirEntries)
    return response.entry


def list_volumes(socket_path, partition, type, repair_tool=False):
    list_info = pb.ListVolumesInfo(partition=int(partition), type=type,
                                   repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/list_volumes', list_info.SerializeToString())
    response = get_rpc_reply(conn, pb.Volumes)
    return response.volumes


def get_volume_stats(socket_path):
    e = pb.Empty()
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/get_volume_stats', e.SerializeToString())
    response = get_rpc_reply(conn, pb.VolumeStats)
    return response.volume


def get_volume(socket_path, index, repair_tool=False):
    volume_idx = pb.VolumeIndex(index=index, repair_tool=repair_tool)
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/get_volume', volume_idx.SerializeToString())
    response = get_rpc_reply(conn, pb.Volume)
    return response


def get_stats(socket_path):
    stats_info = pb.GetStatsInfo()
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/get_stats', stats_info.SerializeToString())
    response = get_rpc_reply(conn, pb.LoadObjectsResponse)
    return response


def get_kv_state(socket_path):
    conn = UnixHTTPConnection(socket_path)
    conn.request('POST', '/get_kv_state')
    response = get_rpc_reply(conn, pb.KvState)
    return response


def set_kv_state(socket_path, isClean):
    conn = UnixHTTPConnection(socket_path)
    newKvState = pb.KvState(isClean=isClean)
    conn.request('POST', '/set_kv_state', newKvState.SerializeToString())
    response = get_rpc_reply(conn, pb.Empty)
    return response
