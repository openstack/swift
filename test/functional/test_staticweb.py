#!/usr/bin/python -u
# Copyright (c) 2010-2017 OpenStack Foundation
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

import functools
import hashlib
import time
from unittest import SkipTest
from urllib.parse import unquote
from swift.common.middleware import tempurl
from swift.common.utils import quote
from swift.common.swob import str_to_wsgi
import test.functional as tf
from test.functional.tests import Utils, Base, Base2, BaseEnv
from test.functional.test_tempurl import tempurl_parms
from test.functional.swift_test_client import Account, Connection, \
    ResponseError


def setUpModule():
    tf.setup_package()


def tearDownModule():
    tf.teardown_package()


def requires_domain_remap(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if 'domain_remap' not in tf.cluster_info:
            raise SkipTest('Domain Remap is not enabled')
        # domain_remap middleware does not advertise its storage_domain values
        # in swift /info responses so a storage_domain must be configured in
        # test.conf for these tests to succeed
        if not tf.config.get('storage_domain'):
            raise SkipTest('Domain Remap storage_domain not configured in %s' %
                           tf.config['__file__'])
        return func(*args, **kwargs)

    return wrapper


class TestStaticWebEnv(BaseEnv):
    static_web_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.static_web_enabled is None:
            cls.static_web_enabled = 'staticweb' in tf.cluster_info
            if not cls.static_web_enabled:
                return

        cls.account = Account(
            cls.conn, tf.config.get('account', tf.config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        if not cls.container.create(
                hdrs={'X-Container-Read': '.r:*,.rlistings'}):
            raise ResponseError(cls.conn.response)

        objects = ['index',
                   'error',
                   'listings_css',
                   'dir/',
                   'dir/obj',
                   'dir/some sub%dir/',
                   'dir/some sub%dir/obj']

        cls.objects = {}
        for item in sorted(objects):
            if '/' in item.rstrip('/'):
                parent, _ = item.rstrip('/').rsplit('/', 1)
                path = '%s/%s' % (cls.objects[parent + '/'].name,
                                  Utils.create_name())
            else:
                path = Utils.create_name()

            if item[-1] == '/':
                cls.objects[item] = cls.container.file(path)
                cls.objects[item].write(hdrs={
                    'Content-Type': 'application/directory'})
            else:
                cls.objects[item] = cls.container.file(path)
                cls.objects[item].write(('%s contents' % item).encode('utf8'))


class TestStaticWeb(Base):
    env = TestStaticWebEnv
    set_up = False

    def setUp(self):
        super(TestStaticWeb, self).setUp()
        if self.env.static_web_enabled is False:
            raise SkipTest("Static Web not enabled")
        elif self.env.static_web_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected static_web_enabled to be True/False, got %r" %
                (self.env.static_web_enabled,))

    @property
    def domain_remap_acct(self):
        # the storage_domain option is test.conf must be set to one of the
        # domain_remap middleware storage_domain values
        return '.'.join((self.env.account.conn.account_name,
                         tf.config.get('storage_domain')))

    @property
    def domain_remap_cont(self):
        # the storage_domain option in test.conf must be set to one of the
        # domain_remap middleware storage_domain values
        return '.'.join(
            (self.env.container.name, self.env.account.conn.account_name,
             tf.config.get('storage_domain')))

    def _set_staticweb_headers(self, index=False, listings=False,
                               listings_css=False, error=False):
        objects = self.env.objects
        headers = {}
        if index:
            headers['X-Container-Meta-Web-Index'] = objects['index'].name
        else:
            headers['X-Remove-Container-Meta-Web-Index'] = 'true'

        if listings:
            headers['X-Container-Meta-Web-Listings'] = 'true'
        else:
            headers['X-Remove-Container-Meta-Web-Listings'] = 'true'

        if listings_css:
            headers['X-Container-Meta-Web-Listings-Css'] = \
                objects['listings_css'].name
        else:
            headers['X-Remove-Container-Meta-Web-Listings-Css'] = 'true'

        if error:
            headers['X-Container-Meta-Web-Error'] = objects['error'].name
        else:
            headers['X-Remove-Container-Meta-Web-Error'] = 'true'

        self.assertTrue(self.env.container.update_metadata(hdrs=headers))

    def _test_redirect_with_slash(self, host, path, anonymous=False):
        self._set_staticweb_headers(listings=True)
        self.env.account.conn.make_request(
            'GET', path,
            hdrs={'X-Web-Mode': str(not anonymous), 'Host': host},
            cfg={'no_auth_token': anonymous, 'absolute_path': True})

        self.assert_status(301)
        expected = '%s://%s%s/' % (
            self.env.account.conn.storage_scheme, host, path)
        self.assertEqual(self.env.conn.response.getheader('location'),
                         expected)

    def _test_redirect_slash_direct(self, anonymous):
        host = self.env.account.conn.storage_netloc
        path = '%s/%s' % (self.env.account.conn.storage_path,
                          quote(self.env.container.name))
        self._test_redirect_with_slash(host, path, anonymous=anonymous)

        path = '%s/%s/%s' % (self.env.account.conn.storage_path,
                             quote(self.env.container.name),
                             quote(self.env.objects['dir/'].name))
        self._test_redirect_with_slash(host, path, anonymous=anonymous)

    def test_redirect_slash_auth_direct(self):
        self._test_redirect_slash_direct(False)

    def test_redirect_slash_anon_direct(self):
        self._test_redirect_slash_direct(True)

    @requires_domain_remap
    def _test_redirect_slash_remap_acct(self, anonymous):
        host = self.domain_remap_acct
        path = '/%s' % quote(self.env.container.name)
        self._test_redirect_with_slash(host, path, anonymous=anonymous)

        path = '/%s/%s' % (quote(self.env.container.name),
                           quote(self.env.objects['dir/'].name))
        self._test_redirect_with_slash(host, path, anonymous=anonymous)

    def test_redirect_slash_auth_remap_acct(self):
        self._test_redirect_slash_remap_acct(False)

    def test_redirect_slash_anon_remap_acct(self):
        self._test_redirect_slash_remap_acct(True)

    @requires_domain_remap
    def _test_redirect_slash_remap_cont(self, anonymous):
        host = self.domain_remap_cont
        path = '/%s' % self.env.objects['dir/'].name
        self._test_redirect_with_slash(host, path, anonymous=anonymous)

    def test_redirect_slash_auth_remap_cont(self):
        self._test_redirect_slash_remap_cont(False)

    def test_redirect_slash_anon_remap_cont(self):
        self._test_redirect_slash_remap_cont(True)

    def _test_get_path(self, host, path, anonymous=False, expected_status=200,
                       expected_in=[], expected_not_in=[]):
        self.env.account.conn.make_request(
            'GET', str_to_wsgi(path),
            hdrs={'X-Web-Mode': str(not anonymous), 'Host': host},
            cfg={'no_auth_token': anonymous, 'absolute_path': True})
        self.assert_status(expected_status)
        body = self.env.account.conn.response.read().decode('utf8')
        for string in expected_in:
            self.assertIn(string, body)
        for string in expected_not_in:
            self.assertNotIn(string, body)

    def _test_listing(self, host, path, title=None, links=[], notins=[],
                      css=None, anonymous=False):
        self._set_staticweb_headers(listings=True,
                                    listings_css=(css is not None))
        if title is None:
            title = unquote(path)
        expected_in = ['Listing of %s' % title] + [
            '<a href="{0}">{1}</a>'.format(quote(link), link)
            for link in links]
        expected_not_in = notins
        if css:
            expected_in.append('<link rel="stylesheet" type="text/css" '
                               'href="%s" />' % quote(css))
        self._test_get_path(host, path, anonymous=anonymous,
                            expected_in=expected_in,
                            expected_not_in=expected_not_in)

    def _test_listing_direct(self, anonymous, listings_css):
        objects = self.env.objects
        host = self.env.account.conn.storage_netloc
        path = '%s/%s/' % (self.env.account.conn.storage_path,
                           quote(self.env.container.name))
        css = objects['listings_css'].name if listings_css else None
        self._test_listing(host, path, anonymous=True, css=css,
                           links=[objects['index'].name,
                                  objects['dir/'].name + '/'],
                           notins=[objects['dir/obj'].name])

        path = '%s/%s/%s/' % (self.env.account.conn.storage_path,
                              quote(self.env.container.name),
                              quote(objects['dir/'].name))
        css = '../%s' % objects['listings_css'].name if listings_css else None
        self._test_listing(
            host, path, anonymous=anonymous, css=css,
            links=[objects['dir/obj'].name.split('/')[-1],
                   objects['dir/some sub%dir/'].name.split('/')[-1] + '/'],
            notins=[objects['index'].name,
                    objects['dir/some sub%dir/obj'].name])

    def test_listing_auth_direct_without_css(self):
        self._test_listing_direct(False, False)

    def test_listing_anon_direct_without_css(self):
        self._test_listing_direct(True, False)

    def test_listing_auth_direct_with_css(self):
        self._test_listing_direct(False, True)

    def test_listing_anon_direct_with_css(self):
        self._test_listing_direct(True, True)

    @requires_domain_remap
    def _test_listing_remap_acct(self, anonymous, listings_css):
        objects = self.env.objects
        host = self.domain_remap_acct
        path = '/%s/' % self.env.container.name
        css = objects['listings_css'].name if listings_css else None
        title = '%s/%s/' % (self.env.account.conn.storage_path,
                            self.env.container.name)
        self._test_listing(host, path, title=title, anonymous=anonymous,
                           css=css,
                           links=[objects['index'].name,
                                  objects['dir/'].name + '/'],
                           notins=[objects['dir/obj'].name])

        path = '/%s/%s/' % (self.env.container.name, objects['dir/'].name)
        css = '../%s' % objects['listings_css'].name if listings_css else None
        title = '%s/%s/%s/' % (self.env.account.conn.storage_path,
                               self.env.container.name,
                               objects['dir/'])
        self._test_listing(
            host, path, title=title, anonymous=anonymous, css=css,
            links=[objects['dir/obj'].name.split('/')[-1],
                   objects['dir/some sub%dir/'].name.split('/')[-1] + '/'],
            notins=[objects['index'].name,
                    objects['dir/some sub%dir/obj'].name])

    def test_listing_auth_remap_acct_without_css(self):
        self._test_listing_remap_acct(False, False)

    def test_listing_anon_remap_acct_without_css(self):
        self._test_listing_remap_acct(True, False)

    def test_listing_auth_remap_acct_with_css(self):
        self._test_listing_remap_acct(False, True)

    def test_listing_anon_remap_acct_with_css(self):
        self._test_listing_remap_acct(True, True)

    @requires_domain_remap
    def _test_listing_remap_cont(self, anonymous, listings_css):
        objects = self.env.objects
        host = self.domain_remap_cont
        path = '/'
        css = objects['listings_css'].name if listings_css else None
        title = '%s/%s/' % (self.env.account.conn.storage_path,
                            self.env.container.name)
        self._test_listing(host, path, title=title, anonymous=anonymous,
                           css=css,
                           links=[objects['index'].name,
                                  objects['dir/'].name + '/'],
                           notins=[objects['dir/obj'].name])

        path = '/%s/' % objects['dir/'].name
        css = '../%s' % objects['listings_css'].name if listings_css else None
        title = '%s/%s/%s/' % (self.env.account.conn.storage_path,
                               self.env.container.name,
                               objects['dir/'])
        self._test_listing(
            host, path, title=title, anonymous=anonymous, css=css,
            links=[objects['dir/obj'].name.split('/')[-1],
                   objects['dir/some sub%dir/'].name.split('/')[-1] + '/'],
            notins=[objects['index'].name,
                    objects['dir/some sub%dir/obj'].name])

    def test_listing_auth_remap_cont_without_css(self):
        self._test_listing_remap_cont(False, False)

    def test_listing_anon_remap_cont_without_css(self):
        self._test_listing_remap_cont(True, False)

    def test_listing_auth_remap_cont_with_css(self):
        self._test_listing_remap_cont(False, True)

    def test_listing_anon_remap_cont_with_css(self):
        self._test_listing_remap_cont(True, True)

    def _test_index(self, host, path, anonymous=False, expected_status=200):
        self._set_staticweb_headers(index=True)
        if expected_status == 200:
            expected_in = ['index contents']
            expected_not_in = ['Listing']
        else:
            expected_in = []
            expected_not_in = []
        self._test_get_path(host, path, anonymous=anonymous,
                            expected_status=expected_status,
                            expected_in=expected_in,
                            expected_not_in=expected_not_in)

    def _test_index_direct(self, anonymous):
        objects = self.env.objects
        host = self.env.account.conn.storage_netloc
        path = '%s/%s/' % (self.env.account.conn.storage_path,
                           quote(self.env.container.name))
        self._test_index(host, path, anonymous=anonymous)

        path = '%s/%s/%s/' % (self.env.account.conn.storage_path,
                              quote(self.env.container.name),
                              quote(objects['dir/'].name))
        self._test_index(host, path, anonymous=anonymous, expected_status=404)

    def test_index_auth_direct(self):
        self._test_index_direct(False)

    def test_index_anon_direct(self):
        self._test_index_direct(True)

    @requires_domain_remap
    def _test_index_remap_acct(self, anonymous):
        objects = self.env.objects
        host = self.domain_remap_acct
        path = '/%s/' % self.env.container.name
        self._test_index(host, path, anonymous=anonymous)

        path = '/%s/%s/' % (self.env.container.name, objects['dir/'].name)
        self._test_index(host, path, anonymous=anonymous, expected_status=404)

    def test_index_auth_remap_acct(self):
        self._test_index_remap_acct(False)

    def test_index_anon_remap_acct(self):
        self._test_index_remap_acct(True)

    @requires_domain_remap
    def _test_index_remap_cont(self, anonymous):
        objects = self.env.objects
        host = self.domain_remap_cont
        path = '/'
        self._test_index(host, path, anonymous=anonymous)

        path = '/%s/' % objects['dir/'].name
        self._test_index(host, path, anonymous=anonymous, expected_status=404)

    def test_index_auth_remap_cont(self):
        self._test_index_remap_cont(False)

    def test_index_anon_remap_cont(self):
        self._test_index_remap_cont(True)


class TestStaticWebUTF8(Base2, TestStaticWeb):
    def test_redirect_slash_auth_remap_cont(self):
        self.skipTest("Can't remap UTF8 containers")

    def test_redirect_slash_anon_remap_cont(self):
        self.skipTest("Can't remap UTF8 containers")


class TestStaticWebTempurlEnv(BaseEnv):
    static_web_enabled = None  # tri-state: None initially, then True/False
    tempurl_enabled = None  # tri-state: None initially, then True/False

    @classmethod
    def setUp(cls):
        cls.conn = Connection(tf.config)
        cls.conn.authenticate()

        if cls.static_web_enabled is None:
            cls.static_web_enabled = 'staticweb' in tf.cluster_info
            if not cls.static_web_enabled:
                return

        if cls.tempurl_enabled is None:
            cls.tempurl_enabled = 'tempurl' in tf.cluster_info
            if not cls.tempurl_enabled:
                return

        cls.account = Account(
            cls.conn, tf.config.get('account', tf.config['username']))
        cls.account.delete_containers()

        cls.container = cls.account.container(Utils.create_name())
        cls.tempurl_key = Utils.create_name()
        if not cls.container.create(
                hdrs={'X-Container-Meta-Web-Listings': 'true',
                      'X-Container-Meta-Temp-URL-Key': cls.tempurl_key}):
            raise ResponseError(cls.conn.response)

        objects = ['index',
                   'error',
                   'listings_css',
                   'dir/',
                   'dir/obj',
                   'dir/subdir/',
                   'dir/subdir/obj']

        cls.objects = {}
        for item in sorted(objects):
            if '/' in item.rstrip('/'):
                parent, _ = item.rstrip('/').rsplit('/', 1)
                path = '%s/%s' % (cls.objects[parent + '/'].name,
                                  Utils.create_name())
            else:
                path = Utils.create_name()

            if item[-1] == '/':
                cls.objects[item] = cls.container.file(path)
                cls.objects[item].write(hdrs={
                    'Content-Type': 'application/directory'})
            else:
                cls.objects[item] = cls.container.file(path)
                cls.objects[item].write(('%s contents' % item).encode('utf8'))


class TestStaticWebTempurl(Base):
    env = TestStaticWebTempurlEnv
    set_up = False

    def setUp(self):
        super(TestStaticWebTempurl, self).setUp()
        if self.env.static_web_enabled is False:
            raise SkipTest("Static Web not enabled")
        elif self.env.static_web_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected static_web_enabled to be True/False, got %r" %
                (self.env.static_web_enabled,))

        if self.env.tempurl_enabled is False:
            raise SkipTest("Temp URL not enabled")
        elif self.env.tempurl_enabled is not True:
            # just some sanity checking
            raise Exception(
                "Expected tempurl_enabled to be True/False, got %r" %
                (self.env.tempurl_enabled,))

        self.whole_container_parms = dict(tempurl_parms(
            'GET', int(time.time() + 60),
            'prefix:%s' % self.env.conn.make_path(
                self.env.container.path + ['']),
            self.env.tempurl_key, hashlib.sha256,
        ), temp_url_prefix='')

    def link(self, virtual_name, parms=None):
        name = self.env.objects[virtual_name].name.rsplit('/', 1)[-1]
        if parms is None:
            parms = self.whole_container_parms
        return (
            '<a href="%s?temp_url_prefix=%s&amp;temp_url_expires=%s&amp;'
            'temp_url_sig=%s">%s</a>' % (
                name,
                parms['temp_url_prefix'],
                quote(str(parms['temp_url_expires'])),
                parms['temp_url_sig'],
                name))

    def test_unauthed(self):
        status = self.env.conn.make_request(
            'GET', self.env.container.path, cfg={'no_auth_token': True})
        self.assertEqual(status, 401)

    def test_staticweb_off(self):
        self.env.container.update_metadata(
            {'X-Remove-Container-Meta-Web-Listings': 'true'})
        status = self.env.conn.make_request(
            'GET', self.env.container.path, parms=self.whole_container_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 401, self.env.conn.response.read())

        status = self.env.conn.make_request(
            'GET', self.env.container.path + [''],
            parms=self.whole_container_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 401)

        status = self.env.conn.make_request(
            'GET',
            self.env.container.path + [self.env.objects['dir/'].name, ''],
            parms=self.whole_container_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 404)

    def test_get_root(self):
        status = self.env.conn.make_request(
            'GET', self.env.container.path, parms=self.whole_container_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 301)

        status = self.env.conn.make_request(
            'GET', self.env.container.path + [''],
            parms=self.whole_container_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 200)
        body = self.env.conn.response.read().decode('utf-8')
        self.assertIn('Listing of /v1/', body)
        self.assertNotIn('href="..', body)
        self.assertIn(self.link('dir/'), body)

    def test_get_dir(self):
        status = self.env.conn.make_request(
            'GET',
            self.env.container.path + [self.env.objects['dir/'].name, ''],
            parms=self.whole_container_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 200)
        body = self.env.conn.response.read().decode('utf-8')
        self.assertIn('Listing of /v1/', body)
        self.assertIn('href="..', body)
        self.assertIn(self.link('dir/obj'), body)
        self.assertIn(self.link('dir/subdir/'), body)

    def test_get_dir_with_iso_expiry(self):
        iso_expiry = time.strftime(
            tempurl.EXPIRES_ISO8601_FORMAT,
            time.gmtime(int(self.whole_container_parms['temp_url_expires'])))
        iso_parms = dict(self.whole_container_parms,
                         temp_url_expires=iso_expiry)
        status = self.env.conn.make_request(
            'GET',
            self.env.container.path + [self.env.objects['dir/'].name, ''],
            parms=iso_parms,
            cfg={'no_auth_token': True})
        self.assertEqual(status, 200)
        body = self.env.conn.response.read().decode('utf-8')
        self.assertIn('Listing of /v1/', body)
        self.assertIn('href="..', body)
        self.assertIn(self.link('dir/obj', iso_parms), body)
        self.assertIn(self.link('dir/subdir/', iso_parms), body)

    def test_get_limited_dir(self):
        parms = dict(tempurl_parms(
            'GET', int(time.time() + 60),
            'prefix:%s' % self.env.conn.make_path(
                self.env.container.path + [self.env.objects['dir/'].name, '']),
            self.env.tempurl_key, hashlib.sha256,
        ), temp_url_prefix=self.env.objects['dir/'].name + '/')
        status = self.env.conn.make_request(
            'GET',
            self.env.container.path + [self.env.objects['dir/'].name, ''],
            parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(status, 200)
        body = self.env.conn.response.read().decode('utf-8')
        self.assertIn('Listing of /v1/', body)
        self.assertNotIn('href="..', body)
        self.assertIn(self.link('dir/obj', parms), body)
        self.assertIn(self.link('dir/subdir/', parms), body)

        status = self.env.conn.make_request(
            'GET', self.env.container.path + [
                self.env.objects['dir/subdir/'].name, ''],
            parms=parms, cfg={'no_auth_token': True})
        self.assertEqual(status, 200)
        body = self.env.conn.response.read().decode('utf-8')
        self.assertIn('Listing of /v1/', body)
        self.assertIn('href="..', body)
        self.assertIn(self.link('dir/subdir/obj', parms), body)
