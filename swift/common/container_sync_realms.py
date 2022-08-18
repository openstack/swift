# Copyright (c) 2013 OpenStack Foundation
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

import errno
import hashlib
import hmac
import os
import time

import configparser

from swift.common.utils import get_valid_utf8_str


class ContainerSyncRealms(object):
    """
    Loads and parses the container-sync-realms.conf, occasionally
    checking the file's mtime to see if it needs to be reloaded.
    """

    def __init__(self, conf_path, logger):
        self.conf_path = conf_path
        self.logger = logger
        self.next_mtime_check = 0
        self.mtime_check_interval = 300
        self.conf_path_mtime = 0
        self.data = {}
        self.reload()

    def reload(self):
        """Forces a reload of the conf file."""
        self.next_mtime_check = 0
        self.conf_path_mtime = 0
        self._reload()

    def _reload(self):
        now = time.time()
        if now >= self.next_mtime_check:
            self.next_mtime_check = now + self.mtime_check_interval
            try:
                mtime = os.path.getmtime(self.conf_path)
            except OSError as err:
                if err.errno == errno.ENOENT:
                    log_func = self.logger.debug
                else:
                    log_func = self.logger.error
                log_func('Could not load %(conf)r: %(error)s', {
                         'conf': self.conf_path, 'error': err})
            else:
                if mtime != self.conf_path_mtime:
                    self.conf_path_mtime = mtime
                    try:
                        conf = configparser.ConfigParser()
                        conf.read(self.conf_path)
                    except configparser.ParsingError as err:
                        self.logger.error(
                            'Could not load %(conf)r: %(error)s',
                            {'conf': self.conf_path, 'error': err})
                    else:
                        try:
                            self.mtime_check_interval = conf.getfloat(
                                'DEFAULT', 'mtime_check_interval')
                            self.next_mtime_check = \
                                now + self.mtime_check_interval
                        except configparser.NoOptionError:
                            self.mtime_check_interval = 300
                            self.next_mtime_check = \
                                now + self.mtime_check_interval
                        except (configparser.ParsingError, ValueError) as err:
                            self.logger.error(
                                'Error in %(conf)r with '
                                'mtime_check_interval: %(error)s',
                                {'conf': self.conf_path, 'error': err})
                        realms = {}
                        for section in conf.sections():
                            realm = {}
                            clusters = {}
                            for option, value in conf.items(section):
                                if option in ('key', 'key2'):
                                    realm[option] = value
                                elif option.startswith('cluster_'):
                                    clusters[option[8:].upper()] = value
                            realm['clusters'] = clusters
                            realms[section.upper()] = realm
                        self.data = realms

    def realms(self):
        """Returns a list of realms."""
        self._reload()
        return list(self.data.keys())

    def key(self, realm):
        """Returns the key for the realm."""
        self._reload()
        result = self.data.get(realm.upper())
        if result:
            result = result.get('key')
        return result

    def key2(self, realm):
        """Returns the key2 for the realm."""
        self._reload()
        result = self.data.get(realm.upper())
        if result:
            result = result.get('key2')
        return result

    def clusters(self, realm):
        """Returns a list of clusters for the realm."""
        self._reload()
        result = self.data.get(realm.upper())
        if result:
            result = result.get('clusters')
            if result:
                result = list(result.keys())
        return result or []

    def endpoint(self, realm, cluster):
        """Returns the endpoint for the cluster in the realm."""
        self._reload()
        result = None
        realm_data = self.data.get(realm.upper())
        if realm_data:
            cluster_data = realm_data.get('clusters')
            if cluster_data:
                result = cluster_data.get(cluster.upper())
        return result

    def get_sig(self, request_method, path, x_timestamp, nonce, realm_key,
                user_key):
        """
        Returns the hexdigest string of the HMAC-SHA1 (RFC 2104) for
        the information given.

        :param request_method: HTTP method of the request.
        :param path: The path to the resource (url-encoded).
        :param x_timestamp: The X-Timestamp header value for the request.
        :param nonce: A unique value for the request.
        :param realm_key: Shared secret at the cluster operator level.
        :param user_key: Shared secret at the user's container level.
        :returns: hexdigest str of the HMAC-SHA1 for the request.
        """
        nonce = get_valid_utf8_str(nonce)
        realm_key = get_valid_utf8_str(realm_key)
        user_key = get_valid_utf8_str(user_key)
        # XXX We don't know what is the best here yet; wait for container
        # sync to be tested.
        if isinstance(path, str):
            path = path.encode('utf-8')
        return hmac.new(
            realm_key,
            b'%s\n%s\n%s\n%s\n%s' % (
                request_method.encode('ascii'), path,
                x_timestamp.encode('ascii'), nonce, user_key),
            hashlib.sha1).hexdigest()
