import errno
import os
import socket
import sys
from ConfigParser import ConfigParser
from httplib import HTTPException
from time import sleep

from swift.common.client import get_auth, http_connection


swift_test_auth = os.environ.get('SWIFT_TEST_AUTH')
swift_test_user = [os.environ.get('SWIFT_TEST_USER'), None, None]
swift_test_key = [os.environ.get('SWIFT_TEST_KEY'), None, None]

# If no environment set, fall back to old school conf file
if not all([swift_test_auth, swift_test_user[0], swift_test_key[0]]):
    conf = ConfigParser()
    class Sectionizer(object):
        def __init__(self, fp):
            self.sent_section = False
            self.fp = fp
        def readline(self):
            if self.sent_section:
                return self.fp.readline()
            self.sent_section = True
            return '[func_test]\n'
    try:
        conf.readfp(Sectionizer(open('/etc/swift/func_test.conf')))
        conf = dict(conf.items('func_test'))
        swift_test_auth = 'http'
        if conf.get('auth_ssl', 'no').lower() in ('yes', 'true', 'on', '1'):
            swift_test_auth = 'https'
        if 'auth_prefix' not in conf:
            conf['auth_prefix'] = '/'
        swift_test_auth += \
            '://%(auth_host)s:%(auth_port)s%(auth_prefix)sv1.0' % conf
        swift_test_user[0] = '%(account)s:%(username)s' % conf
        swift_test_key[0] = conf['password']
        try:
            swift_test_user[1] = '%(account2)s:%(username2)s' % conf
            swift_test_key[1] = conf['password2']
        except KeyError, err:
            pass # old conf, no second account tests can be run
        try:
            swift_test_user[2] = '%(account)s:%(username3)s' % conf
            swift_test_key[2] = conf['password3']
        except KeyError, err:
            pass # old conf, no third account tests can be run
    except IOError, err:
        if err.errno != errno.ENOENT:
            raise

skip = not all([swift_test_auth, swift_test_user[0], swift_test_key[0]])
if skip:
    print >>sys.stderr, 'SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG'

skip2 = not all([not skip, swift_test_user[1], swift_test_key[1]])
if not skip and skip2:
    print >>sys.stderr, \
          'SKIPPING SECOND ACCOUNT FUNCTIONAL TESTS DUE TO NO CONFIG FOR THEM'

skip3 = not all([not skip, swift_test_user[2], swift_test_key[2]])
if not skip and skip3:
    print >>sys.stderr, \
          'SKIPPING THIRD ACCOUNT FUNCTIONAL TESTS DUE TO NO CONFIG FOR THEM'


class AuthError(Exception):
    pass


class InternalServerError(Exception):
    pass


url = [None, None, None]
token = [None, None, None]
parsed = [None, None, None]
conn  = [None, None, None]

def retry(func, *args, **kwargs):
    """
    You can use the kwargs to override the 'retries' (default: 5) and
    'use_account' (default: 1).
    """
    global url, token, parsed, conn
    retries = kwargs.get('retries', 5)
    use_account = 1
    if 'use_account' in kwargs:
        use_account = kwargs['use_account']
        del kwargs['use_account']
    use_account -= 1
    attempts = 0
    backoff = 1
    while attempts <= retries:
        attempts += 1
        try:
            if not url[use_account] or not token[use_account]:
                url[use_account], token[use_account] = \
                    get_auth(swift_test_auth, swift_test_user[use_account],
                             swift_test_key[use_account])
                parsed[use_account] = conn[use_account] = None
            if not parsed[use_account] or not conn[use_account]:
                parsed[use_account], conn[use_account] = \
                    http_connection(url[use_account])
            return func(url[use_account], token[use_account],
                       parsed[use_account], conn[use_account], *args, **kwargs)
        except (socket.error, HTTPException):
            if attempts > retries:
                raise
            parsed[use_account] = conn[use_account] = None
        except AuthError, err:
            url[use_account] = token[use_account] = None
            continue
        except InternalServerError, err:
            pass
        if attempts <= retries:
            sleep(backoff)
            backoff *= 2
    raise Exception('No result after %s retries.' % retries)


def check_response(conn):
    resp = conn.getresponse()
    if resp.status == 401:
        resp.read()
        raise AuthError()
    elif resp.status // 100 == 5:
        resp.read()
        raise InternalServerError()
    return resp
