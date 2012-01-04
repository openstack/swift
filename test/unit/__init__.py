""" Swift tests """

import os
from contextlib import contextmanager
from tempfile import NamedTemporaryFile
from eventlet.green import socket
from tempfile import mkdtemp
from shutil import rmtree


def readuntil2crlfs(fd):
    rv = ''
    lc = ''
    crlfs = 0
    while crlfs < 2:
        c = fd.read(1)
        rv = rv + c
        if c == '\r' and lc != '\n':
            crlfs = 0
        if lc == '\r' and c == '\n':
            crlfs += 1
        lc = c
    return rv


def connect_tcp(hostport):
    rv = socket.socket()
    rv.connect(hostport)
    return rv


@contextmanager
def tmpfile(content):
    with NamedTemporaryFile('w', delete=False) as f:
        file_name = f.name
        f.write(str(content))
    try:
        yield file_name
    finally:
        os.unlink(file_name)

xattr_data = {}


def _get_inode(fd):
    if not isinstance(fd, int):
        try:
            fd = fd.fileno()
        except AttributeError:
            return os.stat(fd).st_ino
    return os.fstat(fd).st_ino


def _setxattr(fd, k, v):
    inode = _get_inode(fd)
    data = xattr_data.get(inode, {})
    data[k] = v
    xattr_data[inode] = data


def _getxattr(fd, k):
    inode = _get_inode(fd)
    data = xattr_data.get(inode, {}).get(k)
    if not data:
        raise IOError
    return data

import xattr
xattr.setxattr = _setxattr
xattr.getxattr = _getxattr


@contextmanager
def temptree(files, contents=''):
    # generate enough contents to fill the files
    c = len(files)
    contents = (list(contents) + [''] * c)[:c]
    tempdir = mkdtemp()
    for path, content in zip(files, contents):
        if os.path.isabs(path):
            path = '.' + path
        new_path = os.path.join(tempdir, path)
        subdir = os.path.dirname(new_path)
        if not os.path.exists(subdir):
            os.makedirs(subdir)
        with open(new_path, 'w') as f:
            f.write(str(content))
    try:
        yield tempdir
    finally:
        rmtree(tempdir)


class FakeLogger(object):
    # a thread safe logger

    def __init__(self, *args, **kwargs):
        self.log_dict = dict(error=[], info=[], warning=[], debug=[])

    def error(self, *args, **kwargs):
        self.log_dict['error'].append((args, kwargs))

    def info(self, *args, **kwargs):
        self.log_dict['info'].append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.log_dict['warning'].append((args, kwargs))

    def debug(self, *args, **kwargs):
        self.log_dict['debug'].append((args, kwargs))


class MockTrue(object):
    """
    Instances of MockTrue evaluate like True
    Any attr accessed on an instance of MockTrue will return a MockTrue instance
    Any method called on an instance of MockTrue will return a MockTrue instance

    >>> thing = MockTrue()
    >>> thing
    True
    >>> thing == True # True == True
    True
    >>> thing == False # True == False
    False
    >>> thing != True # True != True
    False
    >>> thing != False # True != False
    True
    >>> thing.attribute
    True
    >>> thing.method()
    True
    >>> thing.attribute.method()
    True
    >>> thing.method().attribute
    True

    """

    def __getattribute__(self, *args, **kwargs):
        return self
    def __call__(self, *args, **kwargs):
        return self
    def __repr__(*args, **kwargs):
        return repr(True)
    def __eq__(self, other):
        return other is True
    def __ne__(self, other):
        return other is not True
