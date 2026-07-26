# Copyright (c) 2010-2026 OpenStack Foundation
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
import os
import unittest
from unittest import mock
from unittest.mock import MagicMock

import socket

from swift.common.concurrency import USE_EVENTLET

from test import import_gunicorn_or_skip

if not USE_EVENTLET:
    import_gunicorn_or_skip()
    import swift.common.wsgi_gunicorn as wsgi_gunicorn
    from swift.common.wsgi_gunicorn import ChunkedInput, _bind_str, \
        _check_can_bind, _resolve_worker_count, _check_binds_bindable, \
        _binds_and_workers


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestChunkedInput(unittest.TestCase):
    def test_read(self):
        body = MagicMock()
        sock = MagicMock()
        req = MagicMock()

        ci = ChunkedInput(body, sock, req)
        ci.set_hundred_continue_response_headers(
            [('X-Obj-Multiphase-Commit', 'yes'),])
        body.read.return_value = b'data'

        result = ci.read()
        self.assertEqual(result, b'data')

        # Test if headers were sent once
        sent = sock.sendall.call_args[0][0]
        self.assertIn(b'HTTP/1.1 100 Continue\r\n', sent)
        self.assertIn(b'X-Obj-Multiphase-Commit: yes', sent)
        sock.sendall.assert_called_once()

        # Second read, headers should not been sent again
        result = ci.read()
        self.assertEqual(result, b'data')
        sock.sendall.assert_called_once()

        # Test if body is new after send_hundred_continue_response()
        ci.send_hundred_continue_response()
        self.assertIsNot(ci.body, body)


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestBindStr(unittest.TestCase):
    def test_ipv4_unbracketed(self):
        self.assertEqual(_bind_str('0.0.0.0', 6200), '0.0.0.0:6200')
        self.assertEqual(_bind_str('127.0.0.1', 6200), '127.0.0.1:6200')

    def test_ipv6_bracketed(self):
        # gunicorn's parser needs brackets; "::1:6200" / ":::6200" fail
        self.assertEqual(_bind_str('::1', 6200), '[::1]:6200')
        self.assertEqual(_bind_str('::', 6200), '[::]:6200')
        self.assertEqual(_bind_str('fe80::1', 8080), '[fe80::1]:8080')


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestCheckCanBind(unittest.TestCase):
    def test_free_port_ok(self):
        s = socket.socket()
        s.bind(('127.0.0.1', 0))
        port = s.getsockname()[1]
        s.close()
        # no raise
        _check_can_bind('127.0.0.1:%d' % port)

    def test_address_in_use_raises(self):
        held = socket.socket()
        held.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        held.bind(('127.0.0.1', 0))
        held.listen(1)
        try:
            with self.assertRaises(OSError):
                _check_can_bind('127.0.0.1:%d' % held.getsockname()[1])
        finally:
            held.close()


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestTLSHandshakeTimeout(unittest.TestCase):
    def test_ssl_wrap_socket_sets_timeout_before_handshake(self):
        # The handshake runs on a blocking socket inside ssl_wrap_socket; cap
        # it by client_timeout (set on the raw socket before wrapping) so a
        # client that never sends a ClientHello can't pin a gthread forever.
        import gunicorn.sock
        wsgi_gunicorn.patch_gunicorn()
        with mock.patch.object(wsgi_gunicorn, '_CLIENT_TIMEOUT', 12.0):
            raw = MagicMock()
            try:
                gunicorn.sock.ssl_wrap_socket(raw, MagicMock())
            except Exception:
                pass  # orig fails on the mock; we only check the timeout
            raw.settimeout.assert_called_once_with(12.0)


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestResolveWorkerCount(unittest.TestCase):
    # gunicorn workers=0 binds but serves nothing; coerce to 1.
    def test_explicit_count_passes_through(self):
        log = MagicMock()
        self.assertEqual(_resolve_worker_count({'workers': '4'}, log), 4)
        log.warning.assert_not_called()

    def test_auto_uses_cpu_count(self):
        log = MagicMock()
        self.assertEqual(_resolve_worker_count({'workers': 'auto'}, log),
                         wsgi_gunicorn.CPU_COUNT)
        log.warning.assert_not_called()

    def test_zero_coerced_to_one_with_warning(self):
        log = MagicMock()
        self.assertEqual(_resolve_worker_count({'workers': '0'}, log), 1)
        log.warning.assert_called_once()


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestBindsAndWorkers(unittest.TestCase):
    # The servers_per_port mode must be derived from each freshly read conf,
    # not startup state, so a reload can switch it on or off.

    def _conf(self, **kwargs):
        conf = {'bind_ip': '1.2.3.4', 'bind_port': '6200'}
        conf.update(kwargs)
        return conf

    @mock.patch('swift.common.wsgi_gunicorn.BindPortsCache')
    def test_spp_off(self, mock_cache):
        bind, workers = _binds_and_workers(
            self._conf(servers_per_port='0', workers='3'),
            'object-server', MagicMock())
        self.assertEqual('1.2.3.4:6200', bind)
        self.assertEqual(3, workers)
        self.assertFalse(mock_cache.called)

    @mock.patch('swift.common.wsgi_gunicorn.BindPortsCache')
    def test_spp_on(self, mock_cache):
        mock_cache.return_value.all_bind_ports_for_node.return_value = {
            6201, 6200}
        bind, workers = _binds_and_workers(
            self._conf(servers_per_port='2'), 'object-server', MagicMock())
        self.assertEqual(['1.2.3.4:6200', '1.2.3.4:6201'], bind)
        self.assertEqual(4, workers)

    @mock.patch('swift.common.wsgi_gunicorn.BindPortsCache')
    def test_spp_ignored_for_non_object_server(self, mock_cache):
        bind, workers = _binds_and_workers(
            self._conf(servers_per_port='2', workers='3'),
            'container-server', MagicMock())
        self.assertEqual('1.2.3.4:6200', bind)
        self.assertEqual(3, workers)
        self.assertFalse(mock_cache.called)

    @mock.patch('swift.common.wsgi_gunicorn.BindPortsCache')
    def test_mode_follows_each_conf(self, mock_cache):
        # same call sequence a reload produces: off -> on -> off
        mock_cache.return_value.all_bind_ports_for_node.return_value = {6200}
        log = MagicMock()
        bind, workers = _binds_and_workers(
            self._conf(servers_per_port='0', workers='3'),
            'object-server', log)
        self.assertEqual(('1.2.3.4:6200', 3), (bind, workers))
        bind, workers = _binds_and_workers(
            self._conf(servers_per_port='2'), 'object-server', log)
        self.assertEqual((['1.2.3.4:6200'], 2), (bind, workers))
        bind, workers = _binds_and_workers(
            self._conf(servers_per_port='0', workers='3'),
            'object-server', log)
        self.assertEqual(('1.2.3.4:6200', 3), (bind, workers))


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestCheckBindsBindable(unittest.TestCase):
    # --test-config must catch an unbindable bind (a bind_ip not on this host,
    # or a changed bind_port another process owns) -- else SIGHUP drops the
    # working listener then fails to rebind, killing the service. EADDRINUSE is
    # tolerated ONLY for the reload target's own (unchanged) listener.
    def test_eaddrnotavail_propagates(self):
        err = OSError(errno.EADDRNOTAVAIL, 'Cannot assign requested address')
        with mock.patch.object(wsgi_gunicorn, '_check_can_bind',
                               side_effect=err):
            with self.assertRaises(OSError) as cm:
                _check_binds_bindable('10.9.9.9:6200')
            self.assertEqual(cm.exception.errno, errno.EADDRNOTAVAIL)

    def test_eaddrinuse_tolerated_outside_reload(self):
        # no SWIFT_RELOAD_OWNER_SID -> not a reload; tolerate (run-path checks)
        err = OSError(errno.EADDRINUSE, 'Address already in use')
        with mock.patch.dict('os.environ', {}, clear=False):
            os.environ.pop('SWIFT_RELOAD_OWNER_SID', None)
            with mock.patch.object(wsgi_gunicorn, '_check_can_bind',
                                   side_effect=err):
                _check_binds_bindable(['0.0.0.0:6200'])  # no raise

    def test_eaddrinuse_tolerated_when_owned_by_target_session(self):
        # in-use port held by the reload target's own session -> OK
        err = OSError(errno.EADDRINUSE, 'Address already in use')
        with mock.patch.dict('os.environ',
                             {'SWIFT_RELOAD_OWNER_SID': '4242'}), \
                mock.patch.object(wsgi_gunicorn, '_check_can_bind',
                                  side_effect=err), \
                mock.patch.object(wsgi_gunicorn, '_listener_owned_by_session',
                                  return_value=True) as owned:
            _check_binds_bindable(['0.0.0.0:6200'])  # no raise
        owned.assert_called_once_with(6200, 4242)

    def test_eaddrinuse_fails_when_owned_by_other_process(self):
        # changed bind_port onto another process's port -> must fail
        err = OSError(errno.EADDRINUSE, 'Address already in use')
        with mock.patch.dict('os.environ',
                             {'SWIFT_RELOAD_OWNER_SID': '4242'}), \
                mock.patch.object(wsgi_gunicorn, '_check_can_bind',
                                  side_effect=err), \
                mock.patch.object(wsgi_gunicorn, '_listener_owned_by_session',
                                  return_value=False):
            with self.assertRaises(OSError) as cm:
                _check_binds_bindable(['0.0.0.0:6201'])
            self.assertEqual(cm.exception.errno, errno.EADDRINUSE)

    def test_all_addresses_probed(self):
        with mock.patch.object(wsgi_gunicorn, '_check_can_bind') as cb:
            _check_binds_bindable(['a:1', 'b:2', 'c:3'])
        self.assertEqual(cb.call_count, 3)


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestProxyProtocolEnforcement(unittest.TestCase):
    # with require_proxy_protocol on, gunicorn's "auto" mode would serve
    # a connection with no PROXY preamble as plain HTTP; patch_gunicorn makes
    # it reject that (parity with eventlet's SwiftHttpProxiedProtocol).
    def _make_request(self, data, mode='auto'):
        # Constructing a gunicorn Request parses it (Message.__init__ ->
        # parse()), which runs proxy-protocol handling for req_number == 1.
        import gunicorn.http.unreader
        import gunicorn.http.message
        from gunicorn.config import Config
        wsgi_gunicorn.patch_gunicorn()
        cfg = Config()
        cfg.set('proxy_protocol', mode)
        cfg.set('proxy_allow_ips', '*')
        unreader = gunicorn.http.unreader.IterUnreader(iter([data]))
        return gunicorn.http.message.Request(cfg, unreader, ('1.2.3.4', 9))

    def test_missing_preamble_is_rejected(self):
        from gunicorn.http.errors import ForbiddenProxyRequest
        with self.assertRaises(ForbiddenProxyRequest):
            self._make_request(b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')

    def test_valid_preamble_is_accepted(self):
        req = self._make_request(
            b'PROXY TCP4 1.1.1.1 2.2.2.2 100 200\r\n'
            b'GET / HTTP/1.1\r\nHost: x\r\n\r\n')
        self.assertIsNotNone(req.proxy_protocol_info)


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestTuneMalloc(unittest.TestCase):
    def test_pins_thresholds_on_glibc(self):
        # glibc mallopt returns nonzero on success; the helper reports it
        self.assertTrue(wsgi_gunicorn._tune_malloc())

    def test_kill_switch(self):
        with mock.patch.dict('os.environ',
                             {'SWIFT_GTHREAD_NO_MALLOC_TUNE': '1'}):
            self.assertFalse(wsgi_gunicorn._tune_malloc())


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestEnqueueReqCloseOnWorkerThread(unittest.TestCase):
    """Only poller-bound outcomes should cost a trip to the main thread."""

    def setUp(self):
        wsgi_gunicorn.patch_gunicorn()
        from gunicorn.workers.gthread import ThreadWorker
        self.cls = ThreadWorker
        self.worker = ThreadWorker.__new__(ThreadWorker)
        self.worker.alive = True
        self.worker.tpool = MagicMock()
        self.worker.method_queue = MagicMock()
        self.worker.finish_request = MagicMock()
        self.conn = MagicMock()

    def _run(self, result=None, exc=None, cancelled=False, alive=True):
        self.worker.alive = alive
        fut = MagicMock()
        fut.cancelled.return_value = cancelled
        fut.exception.return_value = exc
        fut.result.return_value = result
        # capture the done callback enqueue_req registers, then fire it
        submitted = MagicMock()
        self.worker.tpool.submit.return_value = submitted
        self.cls.enqueue_req(self.worker, self.conn)
        callback = submitted.add_done_callback.call_args[0][0]
        callback(fut)
        return fut

    def assert_inline(self, fut):
        self.worker.method_queue.defer.assert_not_called()
        self.worker.finish_request.assert_called_once_with(self.conn, fut)

    def assert_deferred(self, fut):
        self.worker.finish_request.assert_not_called()
        self.worker.method_queue.defer.assert_called_once_with(
            self.worker.finish_request, self.conn, fut)

    def test_close_runs_on_worker_thread(self):
        # the common case for a Swift backend: proxy closes after one request
        self.assert_inline(self._run(result=False))

    def test_keepalive_goes_to_the_main_thread(self):
        # the connection is handed back to the poller, which the main thread
        # owns, so this one must still be deferred
        self.assert_deferred(self._run(result=True))

    def test_defer_sentinel_goes_to_the_main_thread(self):
        from gunicorn.workers.gthread import _DEFER
        self.assert_deferred(self._run(result=_DEFER))

    def test_failed_request_closes_on_worker_thread(self):
        self.assert_inline(self._run(exc=ValueError('boom')))

    def test_cancelled_request_closes_on_worker_thread(self):
        self.assert_inline(self._run(cancelled=True))

    def test_shutting_down_closes_even_for_keepalive(self):
        # not alive: finish_request's own branch closes rather than
        # re-registering, and that needs no poller
        self.assert_inline(self._run(result=True, alive=False))

    def test_exception_is_not_raised_out_of_the_callback(self):
        # fut.result() would re-raise; we must consult exception() instead so
        # the callback never throws inside the thread pool
        fut = self._run(exc=ValueError('boom'))
        fut.result.assert_not_called()


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestSwiftGunicornApp(unittest.TestCase):
    def _config(self):
        config = MagicMock()
        config.spew = False
        return config

    def test_initial_invalid_config_is_fatal(self):
        logger = MagicMock()
        build_cfg = MagicMock(side_effect=ValueError('bad thread count'))

        with self.assertRaises(SystemExit) as caught:
            wsgi_gunicorn.SwiftGunicornApp(MagicMock(), build_cfg, logger)

        self.assertEqual(1, caught.exception.code)
        logger.exception.assert_not_called()

    def test_reload_keeps_last_good_config_after_error(self):
        logger = MagicMock()
        good_cfg = self._config()
        build_cfg = MagicMock(
            side_effect=[good_cfg, ValueError('bad thread count')])
        app = wsgi_gunicorn.SwiftGunicornApp(
            MagicMock(), build_cfg, logger)

        app.reload()

        self.assertIs(app.cfg, good_cfg)
        self.assertEqual(2, build_cfg.call_count)
        logger.exception.assert_called_once_with(
            'Ignoring invalid configuration during Gunicorn reload')

    def test_reload_replaces_config_after_success(self):
        logger = MagicMock()
        old_cfg = self._config()
        new_cfg = self._config()
        build_cfg = MagicMock(side_effect=[old_cfg, new_cfg])
        app = wsgi_gunicorn.SwiftGunicornApp(
            MagicMock(), build_cfg, logger)

        app.reload()

        self.assertIs(app.cfg, new_cfg)
        logger.exception.assert_not_called()
