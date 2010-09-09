# Copyright (c) 2010 OpenStack, LLC.
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

from __future__ import with_statement
import errno
import os
import socket
from contextlib import contextmanager
from time import gmtime, strftime, time
from urllib import unquote, quote
from uuid import uuid4

import sqlite3
from webob import Request, Response
from webob.exc import HTTPBadRequest, HTTPNoContent, HTTPUnauthorized, \
                      HTTPServiceUnavailable, HTTPNotFound

from swift.common.bufferedhttp import http_connect
from swift.common.db import get_db_connection
from swift.common.ring import Ring
from swift.common.utils import get_logger, normalize_timestamp, split_path


class AuthController(object):
    """
    Sample implementation of an authorization server for development work. This
    server only implements the basic functionality and isn't written for high
    availability or to scale to thousands (or even hundreds) of requests per
    second. It is mainly for use by developers working on the rest of the
    system.

    The design of the auth system was restricted by a couple of existing
    systems.

    This implementation stores an account name, user name, and password (in
    plain text!) as well as a corresponding Swift cluster url and account hash.
    One existing auth system used account, user, and password whereas another
    used just account and an "API key". Here, we support both systems with
    their various, sometimes colliding headers.

    The most common use case is by the end user:

    * The user makes a ReST call to the auth server requesting a token and url
      to use to access the Swift cluster.
    * The auth system validates the user info and returns a token and url for
      the user to use with the Swift cluster.
    * The user makes a ReST call to the Swift cluster using the url given with
      the token as the X-Auth-Token header.
    * The Swift cluster makes an ReST call to the auth server to validate the
      token, caching the result for future requests up to the expiration the
      auth server returns.
    * The auth server validates the token given and returns the expiration for
      the token.
    * The Swift cluster completes the user's request.

    Another use case is creating a new user:

    * The developer makes a ReST call to create a new user.
    * If the account for the user does not yet exist, the auth server makes
      ReST calls to the Swift cluster's account servers to create a new account
      on its end.
    * The auth server records the information in its database.

    A last use case is recreating existing accounts; this is really only useful
    on a development system when the drives are reformatted quite often but
    the auth server's database is retained:

    * A developer makes an ReST call to have the existing accounts recreated.
    * For each account in its database, the auth server makes ReST calls to
      the Swift cluster's account servers to create a specific account on its
      end.

    :param conf: The [auth-server] dictionary of the auth server configuration
                 file
    :param ring: Overrides loading the account ring from a file; useful for
                 testing.

    See the etc/auth-server.conf-sample for information on the possible
    configuration parameters.
    """

    def __init__(self, conf, ring=None):
        self.logger = get_logger(conf)
        self.swift_dir = conf.get('swift_dir', '/etc/swift')
        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH').strip()
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'
        self.default_cluster_url = \
            conf.get('default_cluster_url', 'http://127.0.0.1:8080/v1')
        self.token_life = int(conf.get('token_life', 86400))
        self.log_headers = conf.get('log_headers') == 'True'
        if ring:
            self.account_ring = ring
        else:
            self.account_ring = \
                Ring(os.path.join(self.swift_dir, 'account.ring.gz'))
        self.db_file = os.path.join(self.swift_dir, 'auth.db')
        self.conn = get_db_connection(self.db_file, okay_to_create=True)
        try:
            self.conn.execute('SELECT admin FROM account LIMIT 1')
        except sqlite3.OperationalError, err:
            if str(err) == 'no such column: admin':
                self.conn.execute("ALTER TABLE account ADD COLUMN admin TEXT")
                self.conn.execute("UPDATE account SET admin = 't'")
        self.conn.execute('''CREATE TABLE IF NOT EXISTS account (
                                account TEXT, url TEXT, cfaccount TEXT,
                                user TEXT, password TEXT, admin TEXT)''')
        self.conn.execute('''CREATE INDEX IF NOT EXISTS ix_account_account
                             ON account (account)''')
        try:
            self.conn.execute('SELECT user FROM token LIMIT 1')
        except sqlite3.OperationalError, err:
            if str(err) == 'no such column: user':
                self.conn.execute('DROP INDEX IF EXISTS ix_token_created')
                self.conn.execute('DROP INDEX IF EXISTS ix_token_cfaccount')
                self.conn.execute('DROP TABLE IF EXISTS token')
        self.conn.execute('''CREATE TABLE IF NOT EXISTS token (
                                token TEXT, created FLOAT,
                                account TEXT, user TEXT, cfaccount TEXT)''')
        self.conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_token
                             ON token (token)''')
        self.conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_created
                             ON token (created)''')
        self.conn.execute('''CREATE INDEX IF NOT EXISTS ix_token_account
                             ON token (account)''')
        self.conn.commit()

    def add_storage_account(self, account_name=''):
        """
        Creates an account within the Swift cluster by making a ReST call to
        each of the responsible account servers.

        :param account_name: The desired name for the account; if omitted a
                             UUID4 will be used.
        :returns: False upon failure, otherwise the name of the account
                  within the Swift cluster.
        """
        begin = time()
        orig_account_name = account_name
        if not account_name:
            account_name = '%s%s' % (self.reseller_prefix, uuid4().hex)
        partition, nodes = self.account_ring.get_nodes(account_name)
        headers = {'X-Timestamp': normalize_timestamp(time()),
                   'x-cf-trans-id': 'tx' + str(uuid4())}
        statuses = []
        for node in nodes:
            try:
                conn = None
                conn = http_connect(node['ip'], node['port'], node['device'],
                        partition, 'PUT', '/' + account_name, headers)
                source = conn.getresponse()
                statuses.append(source.status)
                if source.status >= 500:
                    self.logger.error('ERROR With account server %s:%s/%s: '
                        'Response %s %s: %s' %
                        (node['ip'], node['port'], node['device'],
                         source.status, source.reason, source.read(1024)))
                conn = None
            except BaseException, err:
                log_call = self.logger.exception
                msg = 'ERROR With account server ' \
                      '%(ip)s:%(port)s/%(device)s (will retry later): ' % node
                if isinstance(err, socket.error):
                    if err[0] == errno.ECONNREFUSED:
                        log_call = self.logger.error
                        msg += 'Connection refused'
                    elif err[0] == errno.EHOSTUNREACH:
                        log_call = self.logger.error
                        msg += 'Host unreachable'
                log_call(msg)
        rv = False
        if len([s for s in statuses if (200 <= s < 300)]) > len(nodes) / 2:
            rv = account_name
        return rv

    @contextmanager
    def get_conn(self):
        """
        Returns a DB API connection instance to the auth server's SQLite
        database. This is a contextmanager call to be use with the 'with'
        statement. It takes no parameters.
        """
        if not self.conn:
            # We go ahead and make another db connection even if this is a
            # reentry call; just in case we had an error that caused self.conn
            # to become None. Even if we make an extra conn, we'll only keep
            # one after the 'with' block.
            self.conn = get_db_connection(self.db_file)
        conn = self.conn
        self.conn = None
        try:
            yield conn
            conn.rollback()
            self.conn = conn
        except Exception, err:
            try:
                conn.close()
            except:
                pass
            self.conn = get_db_connection(self.db_file)
            raise err

    def purge_old_tokens(self):
        """
        Removes tokens that have expired from the auth server's database. This
        is called by :func:`validate_token` and :func:`GET` to help keep the
        database clean.
        """
        with self.get_conn() as conn:
            conn.execute('DELETE FROM token WHERE created < ?',
                         (time() - self.token_life,))
            conn.commit()

    def validate_token(self, token):
        """
        Tests if the given token is a valid token

        :param token: The token to validate
        :returns: (TTL, account, user, cfaccount) if valid, False otherwise.
                  cfaccount will be None for users without admin access.
        """
        begin = time()
        self.purge_old_tokens()
        rv = False
        with self.get_conn() as conn:
            row = conn.execute('''
                SELECT created, account, user, cfaccount FROM token
                WHERE token = ?''',
                (token,)).fetchone()
            if row is not None:
                created = row[0]
                if time() - created >= self.token_life:
                    conn.execute('''
                        DELETE FROM token WHERE token = ?''', (token,))
                    conn.commit()
                else:
                    rv = (self.token_life - (time() - created), row[1], row[2],
                          row[3])
        self.logger.info('validate_token(%s, _, _) = %s [%.02f]' %
                         (repr(token), repr(rv), time() - begin))
        return rv

    def create_user(self, account, user, password, admin=False):
        """
        Handles the create_user call for developers, used to request a user be
        added in the auth server database. If the account does not yet exist,
        it will be created on the Swift cluster and the details recorded in the
        auth server database.

        The url for the storage account is constructed now and stored
        separately to support changing the configuration file's
        default_cluster_url for directing new accounts to a different Swift
        cluster while still supporting old accounts going to the Swift clusters
        they were created on.

        Currently, updating a user's information (password, admin access) must
        be done by directly updating the sqlite database.

        :param account: The name for the new account
        :param user: The name for the new user
        :param password: The password for the new account
        :param admin: If true, the user will be granted full access to the
                      account; otherwise, another user will have to add the
                      user to the ACLs for containers to grant access.

        :returns: False if the create fails, 'already exists' if the user
                  already exists, or storage url if successful
        """
        begin = time()
        if not all((account, user, password)):
            return False
        with self.get_conn() as conn:
            row = conn.execute(
                'SELECT url FROM account WHERE account = ? AND user = ?',
                (account, user)).fetchone()
            if row:
                self.logger.info(
                    'ALREADY EXISTS create_user(%s, %s, _, %s) [%.02f]' %
                    (repr(account), repr(user), repr(admin),
                     time() - begin))
                return 'already exists'
            row = conn.execute(
                'SELECT url, cfaccount FROM account WHERE account = ?',
                (account,)).fetchone()
            if row:
                url = row[0]
                account_hash = row[1]
            else:
                account_hash = self.add_storage_account()
                if not account_hash:
                    self.logger.info(
                        'FAILED create_user(%s, %s, _, %s) [%.02f]' %
                        (repr(account), repr(user), repr(admin),
                         time() - begin))
                    return False
                url = self.default_cluster_url.rstrip('/') + '/' + account_hash
            conn.execute('''INSERT INTO account
                (account, url, cfaccount, user, password, admin)
                VALUES (?, ?, ?, ?, ?, ?)''',
                (account, url, account_hash, user, password,
                 admin and 't' or ''))
            conn.commit()
        self.logger.info(
            'SUCCESS create_user(%s, %s, _, %s) = %s [%.02f]' %
            (repr(account), repr(user), repr(admin), repr(url),
             time() - begin))
        return url

    def recreate_accounts(self):
        """
        Recreates the accounts from the existing auth database in the Swift
        cluster. This is useful on a development system when the drives are
        reformatted quite often but the auth server's database is retained.

        :returns: A string indicating accounts and failures
        """
        begin = time()
        with self.get_conn() as conn:
            account_hashes = [r[0] for r in conn.execute(
                'SELECT distinct(cfaccount) FROM account').fetchall()]
        failures = []
        for i, account_hash in enumerate(account_hashes):
            if not self.add_storage_account(account_hash):
                failures.append(account_hash)
        rv = '%d accounts, failures %s' % (len(account_hashes), repr(failures))
        self.logger.info('recreate_accounts(_, _) = %s [%.02f]' %
                         (rv, time() - begin))
        return rv

    def handle_token(self, request):
        """
        Handles ReST requests from Swift to validate tokens

        Valid URL paths:
            * GET /token/<token>

        If the HTTP request returns with a 204, then the token is valid, the
        TTL of the token will be available in the X-Auth-Ttl header, and a
        comma separated list of the "groups" the user belongs to will be in the
        X-Auth-Groups header.

        :param request: webob.Request object
        """
        try:
            _, token = split_path(request.path, minsegs=2)
        except ValueError:
            return HTTPBadRequest()
        # Retrieves (TTL, account, user, cfaccount) if valid, False otherwise
        validation = self.validate_token(token)
        if not validation:
            return HTTPNotFound()
        groups = [
            '%s%s:%s' % (self.reseller_prefix, validation[1], validation[2]),
            '%s%s' % (self.reseller_prefix, validation[1])]
        if validation[3]: # admin access to a cfaccount
            groups.append(validation[3])
        return HTTPNoContent(headers={'X-Auth-TTL': validation[0],
                                      'X-Auth-Groups': ','.join(groups)})

    def handle_add_user(self, request):
        """
        Handles Rest requests from developers to have a user added. If the
        account specified doesn't exist, it will also be added. Currently,
        updating a user's information (password, admin access) must be done by
        directly updating the sqlite database.

        Valid URL paths:
            * PUT /account/<account-name>/<user-name> - create the account

        Valid headers:
            * X-Auth-User-Key: <password>
            * X-Auth-User-Admin: <true|false>

        If the HTTP request returns with a 204, then the user was added,
        and the storage url will be available in the X-Storage-Url header.

        :param request: webob.Request object
        """
        try:
            _, account_name, user_name = split_path(request.path, minsegs=3)
        except ValueError:
            return HTTPBadRequest()
        if 'X-Auth-User-Key' not in request.headers:
            return HTTPBadRequest('X-Auth-User-Key is required')
        password = request.headers['x-auth-user-key']
        storage_url = self.create_user(account_name, user_name, password,
                        request.headers.get('x-auth-user-admin') == 'true')
        if storage_url == 'already exists':
            return HTTPBadRequest(storage_url)
        if not storage_url:
            return HTTPServiceUnavailable()
        return HTTPNoContent(headers={'x-storage-url': storage_url})

    def handle_account_recreate(self, request):
        """
        Handles ReST requests from developers to have accounts in the Auth
        system recreated in Swift. I know this is bad ReST style, but this
        isn't production right? :)

        Valid URL paths:
            * POST /recreate_accounts

        :param request: webob.Request object
        """
        result = self.recreate_accounts()
        return Response(result, 200, request=request)

    def handle_auth(self, request):
        """
        Handles ReST requests from end users for a Swift cluster url and auth
        token. This can handle all the various headers and formats that
        existing auth systems used, so it's a bit of a chameleon.

        Valid URL paths:
            * GET /v1/<account-name>/auth
            * GET /auth
            * GET /v1.0

        Valid headers:
            * X-Auth-User: <account-name>:<user-name>
            * X-Auth-Key: <password>
            * X-Storage-User: [<account-name>:]<user-name>
                    The [<account-name>:] is only optional here if the
                    /v1/<account-name>/auth path is used.
            * X-Storage-Pass: <password>

        The (currently) preferred method is to use /v1.0 path and the
        X-Auth-User and X-Auth-Key headers.

        :param request: A webob.Request instance.
        """
        pathsegs = \
            split_path(request.path, minsegs=1, maxsegs=3, rest_with_last=True)
        if pathsegs[0] == 'v1' and pathsegs[2] == 'auth':
            account = pathsegs[1]
            user = request.headers.get('x-storage-user')
            if not user:
                user = request.headers.get('x-auth-user')
                if not user or ':' not in user:
                    return HTTPUnauthorized()
                account2, user = user.split(':', 1)
                if account != account2:
                    return HTTPUnauthorized()
            password = request.headers.get('x-storage-pass')
            if not password:
                password = request.headers.get('x-auth-key')
        elif pathsegs[0] in ('auth', 'v1.0'):
            user = request.headers.get('x-auth-user')
            if not user:
                user = request.headers.get('x-storage-user')
            if not user or ':' not in user:
                return HTTPUnauthorized()
            account, user = user.split(':', 1)
            password = request.headers.get('x-auth-key')
            if not password:
                password = request.headers.get('x-storage-pass')
        else:
            return HTTPBadRequest()
        if not all((account, user, password)):
            return HTTPUnauthorized()
        self.purge_old_tokens()
        with self.get_conn() as conn:
            row = conn.execute('''
                SELECT cfaccount, url, admin FROM account
                WHERE account = ? AND user = ? AND password = ?''',
                (account, user, password)).fetchone()
            if row is None:
                return HTTPUnauthorized()
            cfaccount = row[0]
            url = row[1]
            admin = row[2] == 't'
            row = conn.execute('''
                SELECT token FROM token WHERE account = ? AND user = ?''',
                (account, user)).fetchone()
            if row:
                token = row[0]
            else:
                token = '%stk%s' % (self.reseller_prefix, uuid4().hex)
                conn.execute('''
                    INSERT INTO token
                    (token, created, account, user, cfaccount)
                    VALUES (?, ?, ?, ?, ?)''',
                    (token, time(), account, user, admin and cfaccount or ''))
                conn.commit()
            return HTTPNoContent(headers={'x-auth-token': token,
                                          'x-storage-token': token,
                                          'x-storage-url': url})

    def handleREST(self, env, start_response):
        """
        Handles routing of ReST requests. This handler also logs all requests.

        :param env: WSGI environment
        :param start_response: WSGI start_response function
        """
        req = Request(env)
        logged_headers = None
        if self.log_headers:
            logged_headers = '\n'.join('%s: %s' % (k, v)
                for k, v in req.headers.items()).replace('"', "#042")
        start_time = time()
        # Figure out how to handle the request
        try:
            if req.method == 'GET' and req.path.startswith('/v1') or \
                    req.path.startswith('/auth'):
                handler = self.handle_auth
            elif req.method == 'GET' and req.path.startswith('/token/'):
                handler = self.handle_token
            elif req.method == 'PUT' and req.path.startswith('/account/'):
                handler = self.handle_add_user
            elif req.method == 'POST' and \
                    req.path == '/recreate_accounts':
                handler = self.handle_account_recreate
            else:
                return HTTPBadRequest(request=env)(env, start_response)
            response = handler(req)
        except:
            self.logger.exception('ERROR Unhandled exception in ReST request')
            return HTTPServiceUnavailable(request=req)(env, start_response)
        trans_time = '%.4f' % (time() - start_time)
        if not response.content_length and response.app_iter and \
                    hasattr(response.app_iter, '__len__'):
            response.content_length = sum(map(len, response.app_iter))
        the_request = '%s %s' % (req.method, quote(unquote(req.path)))
        if req.query_string:
            the_request = the_request + '?' + req.query_string
        the_request += ' ' + req.environ['SERVER_PROTOCOL']
        client = req.headers.get('x-cluster-client-ip')
        if not client and 'x-forwarded-for' in req.headers:
            client = req.headers['x-forwarded-for'].split(',')[0].strip()
        if not client:
            client = req.remote_addr
        self.logger.info(
            '%s - - [%s] "%s" %s %s "%s" "%s" - - - - - - - - - "-" "%s" '
            '"%s" %s' % (
                client,
                strftime('%d/%b/%Y:%H:%M:%S +0000', gmtime()),
                the_request,
                response.status_int,
                response.content_length or '-',
                req.referer or '-',
                req.user_agent or '-',
                req.remote_addr,
                logged_headers or '-',
                trans_time))
        return response(env, start_response)

    def __call__(self, env, start_response):
        """ Used by the eventlet.wsgi.server """
        return self.handleREST(env, start_response)

def app_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)
    return AuthController(conf)
