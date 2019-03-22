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
import hashlib
import hmac

from swift.common.exceptions import UnknownSecretIdError
from swift.common.middleware.crypto.crypto_utils import CRYPTO_KEY_CALLBACK
from swift.common.swob import Request, HTTPException, wsgi_to_bytes
from swift.common.utils import readconf, strict_b64decode, get_logger, \
    split_path
from swift.common.wsgi import WSGIContext


class KeyMasterContext(WSGIContext):
    """
    The simple scheme for key derivation is as follows: every path is
    associated with a key, where the key is derived from the path itself in a
    deterministic fashion such that the key does not need to be stored.
    Specifically, the key for any path is an HMAC of a root key and the path
    itself, calculated using an SHA256 hash function::

      <path_key> = HMAC_SHA256(<root_secret>, <path>)
    """
    def __init__(self, keymaster, account, container, obj):
        """
        :param keymaster: a Keymaster instance
        :param account: account name
        :param container: container name
        :param obj: object name
        """
        super(KeyMasterContext, self).__init__(keymaster.app)
        self.keymaster = keymaster
        self.account = account
        self.container = container
        self.obj = obj
        self._keys = {}
        self.alternate_fetch_keys = None

    def _make_key_id(self, path, secret_id, version):
        key_id = {'v': version, 'path': path}
        if secret_id:
            # stash secret_id so that decrypter can pass it back to get the
            # same keys
            key_id['secret_id'] = secret_id
        return key_id

    def fetch_crypto_keys(self, key_id=None, *args, **kwargs):
        """
        Setup container and object keys based on the request path.

        Keys are derived from request path. The 'id' entry in the results dict
        includes the part of the path used to derive keys. Other keymaster
        implementations may use a different strategy to generate keys and may
        include a different type of 'id', so callers should treat the 'id' as
        opaque keymaster-specific data.

        :param key_id: if given this should be a dict with the items included
            under the ``id`` key of a dict returned by this method.
        :returns: A dict containing encryption keys for 'object' and
          'container', and entries 'id' and 'all_ids'. The 'all_ids' entry is a
          list of key id dicts for all root secret ids including the one used
          to generate the returned keys.
        """
        if key_id:
            secret_id = key_id.get('secret_id')
            version = key_id['v']
            if version not in ('1', '2'):
                raise ValueError('Unknown key_id version: %s' % version)
            if version == '1' and not key_id['path'].startswith(
                    '/' + self.account + '/'):
                # Well shoot. This was the bug that made us notice we needed
                # a v2! Hope the current account/container was the original!
                key_acct, key_cont, key_obj = (
                    self.account, self.container, key_id['path'])
            else:
                key_acct, key_cont, key_obj = split_path(
                    key_id['path'], 1, 3, True)

            check_path = (
                self.account, self.container or key_cont, self.obj or key_obj)
            if (key_acct, key_cont, key_obj) != check_path:
                self.keymaster.logger.info(
                    "Path stored in meta (%r) does not match path from "
                    "request (%r)! Using path from meta.",
                    key_id['path'],
                    '/' + '/'.join(x for x in [
                        self.account, self.container, self.obj] if x))
        else:
            secret_id = self.keymaster.active_secret_id
            # v1 had a bug where we would claim the path was just the object
            # name if the object started with a slash. Bump versions to
            # establish that we can trust the path.
            version = '2'
            key_acct, key_cont, key_obj = (
                self.account, self.container, self.obj)

        if (secret_id, version) in self._keys:
            return self._keys[(secret_id, version)]

        keys = {}
        account_path = '/' + key_acct

        try:
            # self.account/container/obj reflect the level of the *request*,
            # which may be different from the level of the key_id-path. Only
            # fetch the keys that the request needs.
            if self.container:
                path = account_path + '/' + key_cont
                keys['container'] = self.keymaster.create_key(
                    path, secret_id=secret_id)

                if self.obj:
                    if key_obj.startswith('/') and version == '1':
                        path = key_obj
                    else:
                        path = path + '/' + key_obj
                    keys['object'] = self.keymaster.create_key(
                        path, secret_id=secret_id)

                # For future-proofing include a keymaster version number and
                # the path used to derive keys in the 'id' entry of the
                # results. The encrypter will persist this as part of the
                # crypto-meta for encrypted data and metadata. If we ever
                # change the way keys are generated then the decrypter could
                # pass the persisted 'id' value when it calls fetch_crypto_keys
                # to inform the keymaster as to how that particular data or
                # metadata had its keys generated. Currently we have no need to
                # do that, so we are simply persisting this information for
                # future use.
                keys['id'] = self._make_key_id(path, secret_id, version)
                # pass back a list of key id dicts for all other secret ids in
                # case the caller is interested, in which case the caller can
                # call this method again for different secret ids; this avoided
                # changing the return type of the callback or adding another
                # callback. Note that the caller should assume no knowledge of
                # the content of these key id dicts.
                keys['all_ids'] = [self._make_key_id(path, id_, version)
                                   for id_ in self.keymaster.root_secret_ids]
                if self.alternate_fetch_keys:
                    alternate_keys = self.alternate_fetch_keys(
                        key_id=None, *args, **kwargs)
                    keys['all_ids'].extend(alternate_keys.get('all_ids', []))

                self._keys[(secret_id, version)] = keys

            return keys
        except UnknownSecretIdError:
            if self.alternate_fetch_keys:
                return self.alternate_fetch_keys(key_id, *args, **kwargs)
            raise

    def handle_request(self, req, start_response):
        self.alternate_fetch_keys = req.environ.get(CRYPTO_KEY_CALLBACK)
        req.environ[CRYPTO_KEY_CALLBACK] = self.fetch_crypto_keys
        resp = self._app_call(req.environ)
        start_response(self._response_status, self._response_headers,
                       self._response_exc_info)
        return resp


class BaseKeyMaster(object):
    """Base middleware for providing encryption keys.

    This provides some basic helpers for:

        - loading from a separate config path,
        - deriving keys based on path, and
        - installing a ``swift.callback.fetch_crypto_keys`` hook
          in the request environment.

    Subclasses should define ``log_route``, ``keymaster_opts``, and
    ``keymaster_conf_section`` attributes, and implement the
    ``_get_root_secret`` function.
    """
    @property
    def log_route(self):
        raise NotImplementedError

    @property
    def keymaster_opts(self):
        raise NotImplementedError

    @property
    def keymaster_conf_section(self):
        raise NotImplementedError

    def _get_root_secret(self, conf):
        raise NotImplementedError

    def __init__(self, app, conf):
        self.app = app
        self.logger = get_logger(conf, log_route=self.log_route)
        self.keymaster_config_path = conf.get('keymaster_config_path')
        conf = self._load_keymaster_config_file(conf)

        # The _get_root_secret() function is overridden by other keymasters
        # which may historically only return a single value
        self._root_secrets = self._get_root_secret(conf)
        if not isinstance(self._root_secrets, dict):
            self._root_secrets = {None: self._root_secrets}
        self.active_secret_id = conf.get('active_root_secret_id') or None
        if self.active_secret_id not in self._root_secrets:
            raise ValueError('No secret loaded for active_root_secret_id %s' %
                             self.active_secret_id)

    @property
    def root_secret(self):
        # Returns the default root secret; this is here for historical reasons
        # to support tests and any third party code that might have used it
        return self._root_secrets.get(self.active_secret_id)

    @property
    def root_secret_ids(self):
        # Only sorted to simplify testing
        return sorted(self._root_secrets.keys(), key=lambda x: x or '')

    def _load_keymaster_config_file(self, conf):
        if not self.keymaster_config_path:
            return conf

        # Keymaster options specified in the filter section would be ignored if
        # a separate keymaster config file is specified. To avoid confusion,
        # prohibit them existing in the filter section.
        bad_opts = []
        for opt in conf:
            for km_opt in self.keymaster_opts:
                if ((km_opt.endswith('*') and opt.startswith(km_opt[:-1])) or
                        opt == km_opt):
                    bad_opts.append(opt)
        if bad_opts:
            raise ValueError('keymaster_config_path is set, but there '
                             'are other config options specified: %s' %
                             ", ".join(bad_opts))
        return readconf(self.keymaster_config_path,
                        self.keymaster_conf_section)

    def _load_multikey_opts(self, conf, prefix):
        result = []
        for k, v in conf.items():
            if not k.startswith(prefix):
                continue
            suffix = k[len(prefix):]
            if suffix and (suffix[0] != '_' or len(suffix) < 2):
                raise ValueError('Malformed root secret option name %s' % k)
            result.append((k, suffix[1:] or None, v))
        return sorted(result)

    def __call__(self, env, start_response):
        req = Request(env)

        try:
            parts = req.split_path(2, 4, True)
        except ValueError:
            return self.app(env, start_response)

        if req.method in ('PUT', 'POST', 'GET', 'HEAD'):
            # handle only those request methods that may require keys
            km_context = KeyMasterContext(self, *parts[1:])
            try:
                return km_context.handle_request(req, start_response)
            except HTTPException as err_resp:
                return err_resp(env, start_response)

        # anything else
        return self.app(env, start_response)

    def create_key(self, path, secret_id=None):
        """
        Creates an encryption key that is unique for the given path.

        :param path: the (WSGI string) path of the resource being encrypted.
        :param secret_id: the id of the root secret from which the key should
            be derived.
        :return: an encryption key.
        :raises UnknownSecretIdError: if the secret_id is not recognised.
        """
        try:
            key = self._root_secrets[secret_id]
        except KeyError:
            self.logger.warning('Unrecognised secret id: %s' % secret_id)
            raise UnknownSecretIdError(secret_id)
        else:
            return hmac.new(key, wsgi_to_bytes(path),
                            digestmod=hashlib.sha256).digest()


class KeyMaster(BaseKeyMaster):
    """Middleware for providing encryption keys.

    The middleware requires its encryption root secret to be set. This is the
    root secret from which encryption keys are derived. This must be set before
    first use to a value that is at least 256 bits. The security of all
    encrypted data critically depends on this key, therefore it should be set
    to a high-entropy value. For example, a suitable value may be obtained by
    generating a 32 byte (or longer) value using a cryptographically secure
    random number generator. Changing the root secret is likely to result in
    data loss.
    """
    log_route = 'keymaster'
    keymaster_opts = ('encryption_root_secret*', 'active_root_secret_id')
    keymaster_conf_section = 'keymaster'

    def _get_root_secret(self, conf):
        """
        This keymaster requires ``encryption_root_secret[_id]`` options to be
        set. At least one must be set before first use to a value that is a
        base64 encoding of at least 32 bytes. The encryption root secrets are
        specified in either proxy-server.conf, or in an external file
        referenced from proxy-server.conf using ``keymaster_config_path``.

        :param conf: the keymaster config section from proxy-server.conf
        :type conf: dict

        :return: a dict mapping secret ids to encryption root secret binary
            bytes
        :rtype: dict
        """
        root_secrets = {}
        for opt, secret_id, value in self._load_multikey_opts(
                conf, 'encryption_root_secret'):
            try:
                secret = self._decode_root_secret(value)
            except ValueError:
                raise ValueError(
                    '%s option in %s must be a base64 encoding of at '
                    'least 32 raw bytes' %
                    (opt, self.keymaster_config_path or 'proxy-server.conf'))
            root_secrets[secret_id] = secret
        return root_secrets

    def _decode_root_secret(self, b64_root_secret):
        binary_root_secret = strict_b64decode(b64_root_secret,
                                              allow_line_breaks=True)
        if len(binary_root_secret) < 32:
            raise ValueError
        return binary_root_secret


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def keymaster_filter(app):
        return KeyMaster(app, conf)

    return keymaster_filter
