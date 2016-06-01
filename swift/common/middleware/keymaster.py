# Copyright (c) 2015 OpenStack Foundation
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

"""
The simple scheme for key derivation is as follows:
every path is associated with a key, where the key is derived from the
path itself in a deterministic fashion such that the key does not need to be
stored. Specifically, the key for any path is an HMAC of a root key and the
path itself, calculated using an SHA256 hash function::

  <path_key> = HMAC_SHA256(<root_secret>, <path>)
"""

import base64
import hashlib
import hmac
import os

from swift.common.middleware.crypto_utils import (
    is_crypto_meta, CRYPTO_KEY_CALLBACK)
from swift.common.request_helpers import get_sys_meta_prefix
from swift.common.swob import Request, HTTPException, HTTPUnprocessableEntity
from swift.common.utils import get_logger, split_path
from swift.common.wsgi import WSGIContext


class KeyMasterContext(WSGIContext):
    def __init__(self, keymaster, account, container, obj):
        """
        :param keymaster: a Keymaster instance
        :param account: account name
        :param container: container name
        :param obj: object name
        """
        super(KeyMasterContext, self).__init__(keymaster.app)
        self.keymaster = keymaster
        self.logger = keymaster.logger
        self.account = account
        self.container = container
        self.obj = obj
        self._init_keys()

    def _init_keys(self):
        """
        Setup default container and object keys based on the request path.
        """
        self.keys = {}
        self.account_path = os.path.join(os.sep, self.account)
        self.container_path = self.obj_path = None
        self.server_type = 'account'

        if self.container:
            self.server_type = 'container'
            self.container_path = os.path.join(self.account_path,
                                               self.container)
            self.keys['container'] = self.keymaster.create_key(
                self.container_path)

            if self.obj:
                self.server_type = 'object'
                self.obj_path = os.path.join(self.container_path, self.obj)
                self.keys['object'] = self.keymaster.create_key(
                    self.obj_path)

    def _handle_post_or_put(self, req, start_response):
        req.environ[CRYPTO_KEY_CALLBACK] = self.fetch_crypto_keys
        resp = self._app_call(req.environ)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp

    def PUT(self, req, start_response):
        if self.obj_path:
            # TODO: re-examine need for this special handling once COPY has
            # been moved to middleware.
            # For object PUT we save a key_id as obj sysmeta so that if the
            # object is copied to another location we can use the key_id
            # (rather than its new path) to calculate its key for a GET or
            # HEAD.
            id_name = "%scrypto-id" % get_sys_meta_prefix(self.server_type)
            req.headers[id_name] = \
                base64.b64encode(self.obj_path)

        return self._handle_post_or_put(req, start_response)

    def POST(self, req, start_response):
        return self._handle_post_or_put(req, start_response)

    def GET(self, req, start_response):
        return self._handle_get_or_head(req, start_response)

    def HEAD(self, req, start_response):
        return self._handle_get_or_head(req, start_response)

    def _handle_get_or_head(self, req, start_response):
        # To get if-match requests working, we now need to provide the keys
        # before we get a response from the object server. There might be
        # a better way of doing this.
        self.provide_keys_get_or_head(req, False)
        resp = self._app_call(req.environ)
        self.provide_keys_get_or_head(req, True)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp

    def error_if_need_keys(self, req):
        # Determine if keys will actually be needed
        # Look for any crypto-meta headers
        if not hasattr(self, '_response_headers'):
            return
        for (h, v) in self._response_headers:
            if is_crypto_meta(h, self.server_type):
                    raise HTTPUnprocessableEntity(
                        "Cannot get keys for path %s" % req.path)

        self.logger.debug("No keys necessary for path %s", req.path)

    def provide_keys_get_or_head(self, req, rederive):
        if rederive and self.obj_path:
            # TODO: re-examine need for this special handling once COPY has
            # been moved to middleware.
            # For object GET or HEAD we look for a key_id that may have been
            # stored in the object sysmeta during a PUT and use that to
            # calculate the object key, in case the object has been copied to a
            # new path.
            try:
                id_name = \
                    "%scrypto-id" % get_sys_meta_prefix(self.server_type)
                obj_key_path = self._response_header_value(id_name)
                if not obj_key_path:
                    raise ValueError('No object key was found.')
                try:
                    obj_key_path = base64.b64decode(obj_key_path)
                except TypeError:
                    self.logger.warning("path %s could not be decoded",
                                        obj_key_path)
                    raise ValueError("path %s could not be decoded" %
                                     obj_key_path)
                path_acc, path_cont, path_obj = \
                    split_path(obj_key_path, 3, 3, True)
                cont_key_path = os.path.join(os.sep, path_acc, path_cont)
                self.keys['container'] = self.keymaster.create_key(
                    cont_key_path)
                self.logger.debug("obj key id: %s", obj_key_path)
                self.logger.debug("cont key id: %s", cont_key_path)
                self.keys['object'] = self.keymaster.create_key(
                    obj_key_path)
            except ValueError:
                req.environ['swift.crypto.override'] = True
                self.error_if_need_keys(req)

        if not req.environ.get('swift.crypto.override'):
            req.environ[CRYPTO_KEY_CALLBACK] = self.fetch_crypto_keys
        else:
            req.environ.pop(CRYPTO_KEY_CALLBACK, None)

    def fetch_crypto_keys(self):
        return self.keys


class KeyMaster(object):

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route="keymaster")
        self.root_secret = conf.get('encryption_root_secret', None)
        if not self.root_secret:
            raise ValueError('encryption_root_secret not set in '
                             'proxy-server.conf')
        self.root_secret = self.root_secret.encode('utf-8')

    def __call__(self, env, start_response):
        req = Request(env)

        try:
            parts = req.split_path(2, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if hasattr(KeyMasterContext, req.method):
            # handle only those request methods that may require keys
            km_context = KeyMasterContext(self, *parts[1:])
            try:
                return getattr(km_context, req.method)(req, start_response)
            except HTTPException as err_resp:
                return err_resp(env, start_response)

        # anything else
        return self.app(env, start_response)

    def create_key(self, key_id):
        return hmac.new(self.root_secret, key_id,
                        digestmod=hashlib.sha256).digest()


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def keymaster_filter(app):
        return KeyMaster(app, conf)

    return keymaster_filter
