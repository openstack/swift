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

import signal
import socket

from swift.common.concurrency import USE_EVENTLET

from test import import_gunicorn_or_skip

if not USE_EVENTLET:
    import_gunicorn_or_skip()
    import swift.common.wsgi_gunicorn as wsgi_gunicorn
    from swift.common.wsgi_gunicorn import ChunkedInput, TopologyChanged, \
        _bind_str, \
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


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestSupervisePerPort(unittest.TestCase):
    """servers_per_port runs one arbiter per port, each bound to a single
    socket, so a wedged disk stalls only its own port's workers.
    """

    def setUp(self):
        self.logger = mock.MagicMock()
        self.handlers = {}
        self.killed = []
        self.pipes = []
        self.ready = []
        self.real_pipe = os.pipe
        self.addCleanup(self._close_pipes)

    def _close_pipes(self):
        for pair in self.pipes:
            for fd in pair:
                try:
                    os.close(fd)
                except OSError:
                    pass

    def _record_kill(self, pid, signum):
        self.killed.append((pid, signum))

    def _pipe(self):
        pair = self.real_pipe()          # os.pipe is patched below
        # the supervisor closes its copy of the write end after forking; keep
        # a dup so a test can play the child and report ready later
        self.pipes.append(pair + (os.dup(pair[1]),))
        return pair

    def _child_reports_ready(self, index=-1, generation=1):
        # an arbiter's workers report the generation of the config they came
        # up on; the supervisor only counts the one it asked for
        os.write(self.pipes[index][2], b'%d\n' % generation)

    def _run(self, ports, pids, waitpids=(), stop_after=1, child_ready=True):
        """Drive the supervisor. `ports` may be a list (static) or a callable.
        `waitpids` is what os.waitpid() returns in turn.
        """
        pid_iter = iter(pids)
        waits = list(waitpids)
        rounds = [0]

        def fake_fork():
            pid = next(pid_iter)
            if child_ready:
                os.write(self.pipes[-1][1], b'1\n')
            return pid

        def fake_select(r, w, x, timeout):
            rounds[0] += 1
            if rounds[0] >= stop_after:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        def fake_waitpid(pid, flags):
            if flags and waits:
                nxt = waits.pop(0)
                return nxt if nxt is not None else (0, 0)
            if flags:
                return (0, 0)
            raise ChildProcessError()

        get_ports = ports if callable(ports) else (lambda: ports)
        desired = lambda: (True, get_ports())
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=fake_fork), \
                mock.patch('os.waitpid', side_effect=fake_waitpid), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=fake_select), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            return wsgi_gunicorn._supervise_per_port(
                desired, self.logger, lambda port, fd: None,
                lambda: self.ready.append(True))

    def _started(self):
        return [c.args[1] for c in self.logger.notice.call_args_list
                if 'Started arbiter' in c.args[0]]

    def test_one_arbiter_per_port(self):
        self._run([6200, 6201, 6202], pids=[100, 101, 102])
        self.assertEqual([6200, 6201, 6202], self._started())

    def test_stop_signal_is_forwarded_to_every_arbiter(self):
        self._run([6200, 6201], pids=[100, 101])
        self.assertEqual([(100, signal.SIGTERM), (101, signal.SIGTERM)],
                         sorted(self.killed))

    def test_a_stop_signal_is_not_swallowed_by_pep_475(self):
        # os.wait() is retried across a handled signal, so the loop has to be
        # woken through the pipe rather than by an interrupted wait
        self._run([6200], pids=[100])
        self.assertEqual([(100, signal.SIGTERM)], self.killed)

    def test_dead_arbiter_is_replaced_on_its_own_port(self):
        # it had been serving (reported ready), so no start-up backoff
        self._run([6200, 6201], pids=[100, 101, 102],
                  waitpids=[None, (101, 0)], stop_after=3)
        self.assertEqual([6200, 6201, 6201], self._started())

    def test_ready_is_reported_once_every_arbiter_is_up(self):
        self._run([6200, 6201], pids=[100, 101])
        self.assertEqual([True], self.ready)

    def test_not_ready_while_an_arbiter_is_still_starting(self):
        self._run([6200, 6201], pids=[100, 101], child_ready=False)
        self.assertEqual([], self.ready)

    def test_reload_starts_an_arbiter_for_a_new_ring_port(self):
        ports = [[6200]]

        def get_ports():
            return ports[0]

        def hup_then_stop(r, w, x, timeout):
            if len(ports[0]) == 1:
                ports[0] = [6200, 6201]        # a new port appears in the ring
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100, 101])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork',
                           side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=hup_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, get_ports()), self.logger,
                lambda port, fd: None, lambda: None)

        self.assertEqual([6200, 6201], self._started())
        self.assertIn((100, signal.SIGHUP), self.killed)

    def test_reload_stops_an_arbiter_whose_port_left_the_ring(self):
        ports = [[6200, 6201]]

        def get_ports():
            return ports[0]

        def hup_then_stop(r, w, x, timeout):
            if len(ports[0]) == 2:
                ports[0] = [6200]
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100, 101])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork',
                           side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=hup_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, get_ports()), self.logger,
                lambda port, fd: None, lambda: None)

        # 6201's arbiter is told to go, 6200's is not restarted
        self.assertEqual([6200, 6201], self._started())
        self.assertIn((101, signal.SIGTERM), self.killed)

    def test_ready_pipes_never_block_the_loop(self):
        # a slow arbiter must not wedge the supervisor in os.read(); PEP 475
        # retries an interrupted read, so a signal could not free it either
        seen = []

        def probe(r, w, x, timeout):
            seen.append(os.get_blocking(self.pipes[-1][0]))
            self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=probe), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200]), self.logger, lambda port, fd: None,
                lambda: None)

        self.assertEqual([False], seen)

    def test_ready_is_reported_again_after_a_reload(self):
        rounds = []

        def hup_then_stop(r, w, x, timeout):
            rounds.append(1)
            if len(rounds) == 1:
                pass                           # let startup readiness land
            elif len(rounds) == 2:
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            elif len(rounds) == 3:
                # the old generation reporting again must not count
                self._child_reports_ready(generation=1)
            elif len(rounds) == 4:
                self._child_reports_ready(generation=2)   # reloaded workers
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100])

        def fork_and_report():
            pid = next(pid_iter)
            self._child_reports_ready()
            return pid

        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=fork_and_report), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=hup_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200]), self.logger, lambda port, fd: None,
                lambda: self.ready.append(True))

        # once at startup, once after the reload
        self.assertEqual(2, len(self.ready))

    def _reload_then_stop(self, desired_seq, pids):
        """Run a reload that swaps what get_desired() returns, then stop."""
        seq = list(desired_seq)
        state = {'i': 0}

        def get_desired():
            return seq[min(state['i'], len(seq) - 1)]

        def hup_then_stop(r, w, x, timeout):
            if state['i'] == 0:
                state['i'] = 1
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter(pids)
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=hup_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                get_desired, self.logger, lambda port, fd: None,
                lambda: self.ready.append(True))

    def test_reload_to_an_empty_ring_stops_the_arbiters(self):
        # this node may legitimately have lost its last local port; that is
        # not the same as the mode being turned off
        self._reload_then_stop([(True, [6200, 6201]), (True, [])],
                               pids=[100, 101])
        self.assertEqual([6200, 6201], self._started())
        self.assertIn((100, signal.SIGTERM), self.killed)
        self.assertIn((101, signal.SIGTERM), self.killed)

    def test_reload_cannot_turn_servers_per_port_off(self):
        # changing topology needs a restart; keep serving what we have
        self._reload_then_stop([(True, [6200, 6201]), (False, [])],
                               pids=[100, 101])
        self.assertEqual([6200, 6201], self._started())
        self.assertIn('restart the server to change topology',
                      self.logger.error.call_args[0][0])

    def test_a_failing_reload_keeps_the_running_arbiters(self):
        calls = [0]

        def get_ports():
            calls[0] += 1
            if calls[0] == 1:
                return [6200]
            raise ValueError('bad servers_per_port')

        def hup_then_stop(r, w, x, timeout):
            if calls[0] == 1:
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=hup_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, get_ports()), self.logger,
                lambda port, fd: None, lambda: None)

        self.assertEqual([6200], self._started())
        self.assertEqual(1, self.logger.exception.call_count)

    def test_sigusr1_is_forwarded_as_itself(self):
        # gunicorn's SIGUSR1 reopens logs; it must not become a full reload
        reloads = [0]

        def usr1_then_stop(r, w, x, timeout):
            reloads[0] += 1
            if reloads[0] == 1:
                self.handlers[signal.SIGUSR1](signal.SIGUSR1, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        ports_read = [0]

        def get_ports():
            ports_read[0] += 1
            return [6200]

        pid_iter = iter([100])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=usr1_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, get_ports()), self.logger,
                lambda port, fd: None, lambda: None)

        self.assertIn((100, signal.SIGUSR1), self.killed)
        self.assertNotIn((100, signal.SIGHUP), self.killed)
        self.assertEqual(1, ports_read[0])      # no reconcile for SIGUSR1

    def test_sigquit_stops_the_arbiters(self):
        # gunicorn's quick-shutdown signal: sent to what used to be the
        # arbiter pid, it must still reach the per-port arbiters
        def quit_now(r, w, x, timeout):
            self.handlers[signal.SIGQUIT](signal.SIGQUIT, None)
            return ([], [], [])

        pid_iter = iter([100, 101])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=quit_now), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200, 6201]), self.logger,
                lambda port, fd: None, lambda: None)

        self.assertEqual([(100, signal.SIGQUIT), (101, signal.SIGQUIT)],
                         sorted(self.killed))

    def test_arbiters_are_stopped_when_the_loop_raises(self):
        # otherwise they keep their listeners and a restart collides
        pid_iter = iter([100, 101])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select',
                           side_effect=RuntimeError('boom')), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            with self.assertRaises(RuntimeError):
                wsgi_gunicorn._supervise_per_port(
                    lambda: (True, [6200, 6201]), self.logger,
                    lambda port, fd: None, lambda: None)

        self.assertEqual([(100, signal.SIGTERM), (101, signal.SIGTERM)],
                         sorted(self.killed))

    def test_no_readiness_while_a_wanted_port_has_no_arbiter(self):
        # a fork that fails must not let the reload report success, and the
        # missing arbiter must be retried without waiting for another SIGHUP
        forks = []

        def flaky_fork():
            forks.append(1)
            if len(forks) == 2:
                raise OSError(errno.EAGAIN, 'cannot fork')
            return 100 + len(forks)

        rounds = []

        def spin(r, w, x, timeout):
            rounds.append(1)
            if len(rounds) >= 3:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        clock = [0.0]

        def tick():
            clock[0] += 5.0          # past any start-up backoff
            return clock[0]

        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=flaky_fork), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('time.monotonic', side_effect=tick), \
                mock.patch('select.select', side_effect=spin), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200, 6201]), self.logger,
                lambda port, fd: None, lambda: self.ready.append(True))

        # 6201 was retried on a later pass rather than left missing
        self.assertEqual([6200, 6201], sorted(self._started()))
        # and readiness was never claimed while it was absent
        self.assertEqual([], self.ready)

    def test_empty_topology_still_reports_ready(self):
        # a node with no local ring ports must still notify a Type=notify
        # service manager, as the eventlet strategy does
        def stop_now(r, w, x, timeout):
            self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=AssertionError), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=stop_now), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, []), self.logger, lambda port, fd: None,
                lambda: self.ready.append(True))

        self.assertEqual([True], self.ready)

    def test_rejected_topology_change_does_not_reload_the_arbiters(self):
        # forwarding HUP first would let every child apply the new config
        self._reload_then_stop([(True, [6200]), (False, [])], pids=[100])
        self.assertNotIn((100, signal.SIGHUP), self.killed)

    def test_a_late_old_generation_report_is_not_readiness(self):
        rounds = []

        def hup_then_stop(r, w, x, timeout):
            rounds.append(1)
            if len(rounds) == 1:
                pass                          # startup readiness lands
            elif len(rounds) == 2:
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            elif len(rounds) == 3:
                # a worker still starting under the old config reports late
                self._child_reports_ready(generation=1)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100])

        def fork_and_report():
            pid = next(pid_iter)
            self._child_reports_ready()
            return pid

        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=fork_and_report), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=hup_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200]), self.logger,
                lambda port, fd: None, lambda: self.ready.append(True))

        # only the startup report counts; the late one is the old config
        self.assertEqual(1, len(self.ready))

    def test_a_failing_arbiter_is_backed_off(self):
        starts = []
        clock = [0.0]

        def fork_fail():
            starts.append(1)
            return 100 + len(starts)

        rounds = []

        def spin(r, w, x, timeout):
            rounds.append(1)
            if len(rounds) >= 4:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        exits = [(101, 1)]

        def waitpid(pid, flags):
            if flags and exits:
                return exits.pop(0)
            if flags:
                return (0, 0)
            raise ChildProcessError()

        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=fork_fail), \
                mock.patch('os.waitpid', side_effect=waitpid), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('time.monotonic', side_effect=lambda: clock[0]), \
                mock.patch('select.select', side_effect=spin), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200]), self.logger,
                lambda port, fd: None, lambda: None)

        # the clock never advances, so the failed port is not re-forked
        self.assertEqual(1, len(starts))

    def test_a_port_that_never_starts_does_not_overflow_the_backoff(self):
        # 2.0 ** 1024 raises OverflowError, and a port reaches that many
        # failures in about a day at the 60s ceiling; the exception would
        # then take every healthy arbiter down with it
        starts = []
        alive = []
        clock = [0.0]

        def fork_fail():
            starts.append(100 + len(starts))
            alive.append(starts[-1])
            return starts[-1]

        def spin(r, w, x, timeout):
            clock[0] += 3600.0           # always past the next retry
            if len(starts) > 1100:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        def waitpid(pid, flags):
            if not flags:
                raise ChildProcessError()
            return (alive.pop(0), 1) if alive else (0, 0)

        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=fork_fail), \
                mock.patch('os.waitpid', side_effect=waitpid), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('time.monotonic', side_effect=lambda: clock[0]), \
                mock.patch('select.select', side_effect=spin), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200]), self.logger,
                lambda port, fd: None, lambda: None)

        self.assertGreater(len(starts), 1024)
        delays = [call[0][-1] for call in self.logger.error.call_args_list
                  if 'next try' in call[0][0]]
        self.assertEqual(60.0, max(delays))

    def test_a_burst_of_hangups_is_one_reload(self):
        # a child may see a single delivery for several HUPs, so arming for
        # more generations than it will reach would hang the reload
        rounds = []

        def hup_twice_then_stop(r, w, x, timeout):
            rounds.append(1)
            if len(rounds) == 1:
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
                self.handlers[signal.SIGHUP](signal.SIGHUP, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        reloads = []
        pid_iter = iter([100])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select',
                           side_effect=hup_twice_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            def get_desired():
                reloads.append(1)
                return (True, [6200])

            wsgi_gunicorn._supervise_per_port(
                get_desired, self.logger, lambda port, fd: None, lambda: None)

        self.assertEqual(
            1, len([s for p, s in self.killed if s == signal.SIGHUP]))
        self.assertEqual(2, len(reloads))     # startup, then the one reload

    def test_ttin_and_ttou_reach_the_arbiters(self):
        sent = []

        def signals_then_stop(r, w, x, timeout):
            if not sent:
                sent.append(1)
                self.handlers[signal.SIGTTIN](signal.SIGTTIN, None)
                self.handlers[signal.SIGTTOU](signal.SIGTTOU, None)
            else:
                self.handlers[signal.SIGTERM](signal.SIGTERM, None)
            return ([], [], [])

        pid_iter = iter([100])
        with mock.patch('os.pipe', side_effect=self._pipe), \
                mock.patch('os.fork', side_effect=lambda: next(pid_iter)), \
                mock.patch('os.waitpid', side_effect=ChildProcessError), \
                mock.patch('os.kill', side_effect=self._record_kill), \
                mock.patch('select.select', side_effect=signals_then_stop), \
                mock.patch('signal.set_wakeup_fd', return_value=-1), \
                mock.patch('signal.signal',
                           side_effect=lambda n, h: self.handlers.__setitem__(
                               n, h)):
            wsgi_gunicorn._supervise_per_port(
                lambda: (True, [6200]), self.logger,
                lambda port, fd: None, lambda: None)

        self.assertIn((100, signal.SIGTTIN), self.killed)
        self.assertIn((100, signal.SIGTTOU), self.killed)


class _BuildCfgHarness(unittest.TestCase):
    """Runs run_wsgi far enough to grab the real build_cfg closure, so a
    test can drive a reload the way SwiftGunicornApp.reload() does.
    """

    def _capture_build_cfg(self, conf_values):
        captured = {}

        class FakeApp(object):
            def __init__(self, load_app, build_cfg, logger):
                captured['build_cfg'] = build_cfg

            def run(self):
                pass

        conf = {'__file__': 'x.conf'}
        patches = [
            mock.patch.object(wsgi_gunicorn, 'check_config_gunicorn',
                              return_value=(conf, mock.MagicMock(), {})),
            mock.patch.object(wsgi_gunicorn, 'SwiftGunicornApp', FakeApp),
            mock.patch.object(wsgi_gunicorn, 'appconfig',
                              side_effect=lambda *a, **kw: conf_values[0]),
            mock.patch.object(wsgi_gunicorn.constraints,
                              'reload_constraints'),
            mock.patch.object(wsgi_gunicorn, 'reload_storage_policies'),
            mock.patch.object(wsgi_gunicorn, '_binds_and_workers',
                              return_value=('0.0.0.0:6200', 2)),
            mock.patch.object(wsgi_gunicorn, 'clean_up_daemon_hygiene'),
            mock.patch.object(wsgi_gunicorn, '_tune_malloc'),
            mock.patch.object(wsgi_gunicorn, '_check_binds_bindable'),
            mock.patch.object(wsgi_gunicorn, '_check_can_bind'),
            mock.patch.object(wsgi_gunicorn, 'capture_stdio'),
            mock.patch.object(wsgi_gunicorn, 'systemd_notify'),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        wsgi_gunicorn.run_wsgi('x.conf', 'object-server')
        self.reload_constraints = wsgi_gunicorn.constraints.reload_constraints
        self.reload_policies = wsgi_gunicorn.reload_storage_policies
        return captured['build_cfg']


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestTopologyIsFixedAtStartup(_BuildCfgHarness):
    """One arbiter per port or a single arbiter is decided at startup; a
    reload that flips servers_per_port would leave one arbiter holding every
    listener, so it is rejected instead.
    """

    def test_a_topology_change_is_the_one_reload_error_not_swallowed(self):
        app = wsgi_gunicorn.SwiftGunicornApp.__new__(
            wsgi_gunicorn.SwiftGunicornApp)
        app.swift_logger = mock.MagicMock()
        app.cfg = 'the running config'

        app.build_cfg = mock.Mock(side_effect=TopologyChanged('nope'))
        with self.assertRaises(TopologyChanged):
            app.reload()          # the arbiter has to see this one
        self.assertEqual('the running config', app.cfg)

        app.build_cfg = mock.Mock(side_effect=ValueError('bad bind'))
        app.reload()              # any other bad edit is just logged
        self.assertEqual('the running config', app.cfg)
        self.assertTrue(app.swift_logger.exception.called)


@unittest.skipIf(USE_EVENTLET, 'gunicorn is only used without eventlet')
class TestReloadValidatesBeforeApplying(_BuildCfgHarness):
    """A reload gunicorn goes on to reject must not have changed anything
    outside the config it was building.
    """

    def test_a_bad_value_leaves_the_running_settings_alone(self):
        holder = [{'client_timeout': '30'}]
        build_cfg = self._capture_build_cfg(holder)
        build_cfg()
        self.assertEqual(30, wsgi_gunicorn._CLIENT_TIMEOUT)
        before = (self.reload_constraints.call_count,
                  self.reload_policies.call_count)

        holder[0] = {'client_timeout': '90', 'threads': 'sixteen'}
        with self.assertRaises(ValueError):
            build_cfg()

        self.assertEqual(30, wsgi_gunicorn._CLIENT_TIMEOUT)
        self.assertEqual(before, (self.reload_constraints.call_count,
                                  self.reload_policies.call_count))

    def test_constraints_read_by_the_reload_reach_the_request_limits(self):
        original = wsgi_gunicorn.constraints.MAX_HEADER_SIZE
        self.addCleanup(setattr, wsgi_gunicorn.constraints,
                        'MAX_HEADER_SIZE', original)
        build_cfg = self._capture_build_cfg([{}])
        self.reload_constraints.side_effect = lambda: setattr(
            wsgi_gunicorn.constraints, 'MAX_HEADER_SIZE', original + 100)

        cfg = build_cfg()

        self.assertEqual(original + 99, cfg.limit_request_field_size)

    def test_a_rejected_reload_never_reaches_the_workers(self):
        # Gunicorn asks the app for its new config before it touches
        # anything, so refusing there leaves the running server alone
        arbiter = wsgi_gunicorn._SwiftArbiter.__new__(
            wsgi_gunicorn._SwiftArbiter)
        arbiter._stats = {'reloads': 0}
        arbiter.log = mock.MagicMock()
        arbiter.cfg = mock.MagicMock(env={}, address=[])
        arbiter.app = mock.MagicMock()
        arbiter.app.reload.side_effect = TopologyChanged('needs a restart')
        touched = []
        arbiter.setup = lambda app: touched.append('setup')
        arbiter.spawn_worker = lambda: touched.append('spawn_worker')
        arbiter.manage_workers = lambda: touched.append('manage_workers')

        arbiter.reload()

        self.assertEqual([], touched)
        self.assertIn('needs a restart',
                      str(arbiter.log.error.call_args[0][-1]))

    def test_only_a_config_that_built_is_a_new_generation(self):
        holder = [{'servers_per_port': '0'}]
        build_cfg = self._capture_build_cfg(holder)
        read_fd, write_fd = os.pipe()
        self.addCleanup(os.close, read_fd)
        self.addCleanup(os.close, write_fd)

        def report(cfg):
            cfg.post_worker_init(mock.MagicMock())
            return os.read(read_fd, 64)

        running = build_cfg(6200, write_fd)
        first = report(running)

        holder[0] = {'servers_per_port': '0', 'threads': 'sixteen'}
        with self.assertRaises(ValueError):
            build_cfg(6200, write_fd)
        # gunicorn respawns the workers from the config it kept
        self.assertEqual(first, report(running))

        holder[0] = {'servers_per_port': '0'}
        self.assertNotEqual(first, report(build_cfg(6200, write_fd)))

    def test_reload_cannot_turn_servers_per_port_on(self):
        # the documented migration is enable-then-SIGHUP; under gunicorn that
        # would silently give one arbiter every listener
        holder = [{'servers_per_port': '0'}]
        build_cfg = self._capture_build_cfg(holder)
        build_cfg()                       # the running topology still applies

        holder[0] = {'servers_per_port': '2'}
        with self.assertRaises(TopologyChanged) as caught:
            build_cfg()
        self.assertIn('restart the server', str(caught.exception))
