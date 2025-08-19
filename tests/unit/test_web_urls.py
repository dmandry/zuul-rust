# Copyright 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import json
import urllib

from bs4 import BeautifulSoup
import testtools

from tests.base import ZuulTestCase, WebProxyFixture
from tests.base import ZuulWebFixture


class TestWebURLs(ZuulTestCase):
    tenant_config_file = 'config/single-tenant/main.yaml'

    def setUp(self):
        super(TestWebURLs, self).setUp()
        self.web = self.useFixture(
            ZuulWebFixture(self.config, self.test_config,
                           self.additional_event_queues, self.upstream_root,
                           self.poller_events,
                           self.git_url_with_auth, self.addCleanup,
                           self.test_root))

    def _get(self, port, uri):
        url = "http://localhost:{}{}".format(port, uri)
        self.log.debug("GET {}".format(url))
        req = urllib.request.Request(url)
        try:
            f = urllib.request.urlopen(req)
        except urllib.error.HTTPError:
            raise Exception("Error on URL {}".format(url))
        return f.read()

    def _crawl(self, url):
        page = self._get(self.port, url)
        page = BeautifulSoup(page, 'html.parser')
        for (tag, attr) in [
                ('script', 'src'),
                ('link', 'href'),
                ('a', 'href'),
                ('img', 'src'),
        ]:
            for item in page.find_all(tag):
                suburl = item.get(attr)
                if tag == 'script' and suburl is None:
                    # There can be an embedded script
                    continue
                if suburl.startswith('/'):
                    suburl = suburl[1:]
                link = urllib.parse.urljoin(url, suburl)
                self._get(self.port, link)


class TestDirect(TestWebURLs):
    # Test directly accessing the zuul-web server with no proxy
    def setUp(self):
        super(TestDirect, self).setUp()
        self.port = self.web.port

    def test_status_page(self):
        self._crawl('/')
        self._crawl('/t/tenant-one/status')

    def test_forbidden(self):
        for url in [
                '/?path=/etc/os-release',
                '/?path=../__init__.py',
                '/../__init__.py',
        ]:
            with testtools.ExpectedException(Exception):
                self._get(self.port, url).decode('utf-8')
        index = self._get(self.port, '/zuul/').decode('utf-8')
        for url in [
                '/etc/os-release',
                '//etc/os-release',
                '/%2fetc%2fos-release',
                '/%2e%2e%2f__init__.py',
        ]:
            data = self._get(self.port, url).decode('utf-8')
            self.assertEqual(index, data)


class TestWhiteLabel(TestWebURLs):
    # Test a zuul-web behind a whitelabel proxy (i.e., what
    # zuul.openstack.org does).
    def setUp(self):
        super(TestWhiteLabel, self).setUp()
        rules = [
            ('^/(.*)$', 'http://localhost:{}/\\1'.format(self.web.port)),
        ]
        self.proxy = self.useFixture(WebProxyFixture(rules))
        self.port = self.proxy.port

    def test_status_page(self):
        self._crawl('/')
        self._crawl('/status')

    def test_forbidden(self):
        for url in [
                '/?path=/etc/os-release',
                '/?path=../__init__.py',
        ]:
            with testtools.ExpectedException(Exception):
                self._get(self.port, url).decode('utf-8')
        index = self._get(self.port, '/zuul/').decode('utf-8')
        for url in [
                '/etc/os-release',
                '//etc/os-release',
                '/%2fetc%2fos-release',
                '/../__init__.py',
                '/%2e%2e%2f__init__.py',
        ]:
            data = self._get(self.port, url).decode('utf-8')
            self.assertEqual(index, data)


class TestWhiteLabelAPI(TestWebURLs):
    # Test a zuul-web behind a whitelabel proxy (i.e., what
    # zuul.openstack.org does).
    def setUp(self):
        super(TestWhiteLabelAPI, self).setUp()
        rules = [
            ('^/api/(.*)$',
             'http://localhost:{}/api/tenant/tenant-one/\\1'.format(
                 self.web.port)),
            ('^/oidc/(.*)$',
             'http://localhost:{}/oidc/tenant/tenant-one/\\1'.format(
                 self.web.port)),
            ('^/.well-known/openid-configuration$',
             'http://localhost:{}/oidc/tenant/tenant-one/'
             '.well-known/openid-configuration'.format(
                 self.web.port)),
        ]
        self.proxy = self.useFixture(WebProxyFixture(rules))
        self.port = self.proxy.port

    def test_info(self):
        info = json.loads(self._get(self.port, '/api/info').decode('utf-8'))
        self.assertEqual('tenant-one', info['info']['tenant'])

    def test_oidc(self):
        openid = json.loads(self._get(
            self.port, '/.well-known/openid-configuration').decode('utf-8'))
        self.assertEqual(openid['issuer'], 'https://zuul.example.com')
        self.assertEqual(openid['jwks_uri'],
                         'https://zuul.example.com/oidc/jwks')
        jwks = json.loads(self._get(
            self.port, '/oidc/jwks').decode('utf-8'))
        self.assertGreater(len(jwks["keys"]), 0)


class TestSuburl(TestWebURLs):
    # Test a zuul-web mounted on a suburl (i.e., what software factory
    # does).
    def setUp(self):
        super(TestSuburl, self).setUp()
        rules = [
            ('^/zuul/(.*)$', 'http://localhost:{}/\\1'.format(
                self.web.port)),
        ]
        self.proxy = self.useFixture(WebProxyFixture(rules))
        self.port = self.proxy.port

    def test_status_page(self):
        self._crawl('/zuul/')

    def test_forbidden(self):
        for url in [
                '/zuul/?path=/etc/os-release',
                '/zuul/?path=../__init__.py',
        ]:
            with testtools.ExpectedException(Exception):
                self._get(self.port, url).decode('utf-8')
        index = self._get(self.port, '/zuul/').decode('utf-8')
        for url in [
                '/zuul/etc/os-release',
                '/zuul//etc/os-release',
                '/zuul/%2fetc%2fos-release',
                '/zuul/../__init__.py',
                '/zuul/%2e%2e%2f__init__.py',
        ]:
            data = self._get(self.port, url).decode('utf-8')
            self.assertEqual(index, data)
