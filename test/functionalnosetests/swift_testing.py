import errno
import os
import socket
import sys
from ConfigParser import ConfigParser
from httplib import HTTPException
from time import sleep

from swift.common.client import get_auth, http_connection


swift_test_auth = os.environ.get('SWIFT_TEST_AUTH')
swift_test_user = os.environ.get('SWIFT_TEST_USER')
swift_test_key = os.environ.get('SWIFT_TEST_KEY')

# If no environment set, fall back to old school conf file
if not all([swift_test_auth, swift_test_user, swift_test_key]):
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
        swift_test_auth += '://%(auth_host)s:%(auth_port)s/v1.0' % conf
        swift_test_user = '%(account)s:%(username)s' % conf
        swift_test_key = conf['password']
    except IOError, err:
        if err.errno != errno.ENOENT:
            raise

skip = not all([swift_test_auth, swift_test_user, swift_test_key])
if skip:
    print >>sys.stderr, 'SKIPPING FUNCTIONAL TESTS DUE TO NO CONFIG'


class AuthError(Exception):
    pass


class InternalServerError(Exception):
    pass


url = token = parsed = conn = None

def retry(func, *args, **kwargs):
    global url, token, parsed, conn
    retries = kwargs.get('retries', 5)
    attempts = 0
    backoff = 1
    while attempts <= retries:
        attempts += 1
        try:
            if not url or not token:
                url, token = \
                    get_auth(swift_test_auth, swift_test_user, swift_test_key)
                parsed = conn = None
            if not parsed or not conn:
                parsed, conn = http_connection(url)
            return func(url, token, parsed, conn, *args, **kwargs)
        except (socket.error, HTTPException):
            if attempts > retries:
                raise
            parsed = conn = None
        except AuthError, err:
            url = token = None
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
