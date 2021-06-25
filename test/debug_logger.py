# Copyright (c) 2010-2021 OpenStack Foundation
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
import contextlib
import logging
import mock
import sys

from collections import defaultdict

from swift.common import utils
from swift.common.utils import NOTICE


class WARN_DEPRECATED(Exception):
    def __init__(self, msg):
        self.msg = msg
        print(self.msg)


class CaptureLog(object):
    """
    Captures log records passed to the ``handle`` method and provides accessor
    functions to the captured logs.
    """
    def __init__(self):
        self.clear()

    def _clear(self):
        self.log_dict = defaultdict(list)
        self.lines_dict = {'critical': [], 'error': [], 'info': [],
                           'warning': [], 'debug': [], 'notice': []}

    clear = _clear  # this is a public interface

    def get_lines_for_level(self, level):
        if level not in self.lines_dict:
            raise KeyError(
                "Invalid log level '%s'; valid levels are %s" %
                (level,
                 ', '.join("'%s'" % lvl for lvl in sorted(self.lines_dict))))
        return self.lines_dict[level]

    def all_log_lines(self):
        return dict((level, msgs) for level, msgs in self.lines_dict.items()
                    if len(msgs) > 0)

    def _handle(self, record):
        try:
            line = record.getMessage()
        except TypeError:
            print('WARNING: unable to format log message %r %% %r' % (
                record.msg, record.args))
            raise
        self.lines_dict[record.levelname.lower()].append(line)
        return 0

    def handle(self, record):
        return self._handle(record)


class FakeLogger(logging.Logger, CaptureLog):
    # a thread safe fake logger

    def __init__(self, *args, **kwargs):
        self._clear()
        self.name = 'swift.unit.fake_logger'
        self.level = logging.NOTSET
        if 'facility' in kwargs:
            self.facility = kwargs['facility']
        self.statsd_client = None
        self.thread_locals = None
        self.parent = None

    store_in = {
        logging.ERROR: 'error',
        logging.WARNING: 'warning',
        logging.INFO: 'info',
        logging.DEBUG: 'debug',
        logging.CRITICAL: 'critical',
        NOTICE: 'notice',
    }

    def warn(self, *args, **kwargs):
        raise WARN_DEPRECATED("Deprecated Method warn use warning instead")

    def notice(self, msg, *args, **kwargs):
        """
        Convenience function for syslog priority LOG_NOTICE. The python
        logging lvl is set to 25, just above info.  SysLogHandler is
        monkey patched to map this log lvl to the LOG_NOTICE syslog
        priority.
        """
        self.log(NOTICE, msg, *args, **kwargs)

    def _log(self, level, msg, *args, **kwargs):
        store_name = self.store_in[level]
        cargs = [msg]
        if any(args):
            cargs.extend(args)
        captured = dict(kwargs)
        if 'exc_info' in kwargs and \
                not isinstance(kwargs['exc_info'], tuple):
            captured['exc_info'] = sys.exc_info()
        self.log_dict[store_name].append((tuple(cargs), captured))
        super(FakeLogger, self)._log(level, msg, *args, **kwargs)

    def _store_in(store_name):
        def stub_fn(self, *args, **kwargs):
            self.log_dict[store_name].append((args, kwargs))
        return stub_fn

    # mock out the StatsD logging methods:
    update_stats = _store_in('update_stats')
    increment = _store_in('increment')
    decrement = _store_in('decrement')
    timing = _store_in('timing')
    timing_since = _store_in('timing_since')
    transfer_rate = _store_in('transfer_rate')
    set_statsd_prefix = _store_in('set_statsd_prefix')

    def get_increments(self):
        return [call[0][0] for call in self.log_dict['increment']]

    def get_increment_counts(self):
        counts = {}
        for metric in self.get_increments():
            if metric not in counts:
                counts[metric] = 0
            counts[metric] += 1
        return counts

    def setFormatter(self, obj):
        self.formatter = obj

    def close(self):
        self._clear()

    def set_name(self, name):
        # don't touch _handlers
        self._name = name

    def acquire(self):
        pass

    def release(self):
        pass

    def createLock(self):
        pass

    def emit(self, record):
        pass

    def flush(self):
        pass

    def handleError(self, record):
        pass

    def isEnabledFor(self, level):
        return True


class DebugSwiftLogFormatter(utils.SwiftLogFormatter):

    def format(self, record):
        msg = super(DebugSwiftLogFormatter, self).format(record)
        return msg.replace('#012', '\n')


class DebugLogger(FakeLogger):
    """A simple stdout logging version of FakeLogger"""

    def __init__(self, *args, **kwargs):
        FakeLogger.__init__(self, *args, **kwargs)
        self.formatter = DebugSwiftLogFormatter(
            "%(server)s %(levelname)s: %(message)s")
        self.records = defaultdict(list)

    def handle(self, record):
        self._handle(record)
        formatted = self.formatter.format(record)
        print(formatted)
        self.records[record.levelname].append(formatted)


class DebugLogAdapter(utils.LogAdapter):

    def _send_to_logger(name):
        def stub_fn(self, *args, **kwargs):
            return getattr(self.logger, name)(*args, **kwargs)
        return stub_fn

    # delegate to FakeLogger's mocks
    update_stats = _send_to_logger('update_stats')
    increment = _send_to_logger('increment')
    decrement = _send_to_logger('decrement')
    timing = _send_to_logger('timing')
    timing_since = _send_to_logger('timing_since')
    transfer_rate = _send_to_logger('transfer_rate')
    set_statsd_prefix = _send_to_logger('set_statsd_prefix')

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return getattr(self.__dict__['logger'], name)


def debug_logger(name='test'):
    """get a named adapted debug logger"""
    return DebugLogAdapter(DebugLogger(), name)


class ForwardingLogHandler(logging.NullHandler):
    """
    Provides a LogHandler implementation that simply forwards filtered records
    to a given handler function. This can be useful to forward records to a
    handler without the handler itself needing to subclass LogHandler.
    """
    def __init__(self, handler_fn):
        super(ForwardingLogHandler, self).__init__()
        self.handler_fn = handler_fn

    def handle(self, record):
        return self.handler_fn(record)


class CaptureLogAdapter(utils.LogAdapter, CaptureLog):
    """
    A LogAdapter that is capable of capturing logs for inspection via accessor
    methods.
    """
    def __init__(self, logger, name):
        super(CaptureLogAdapter, self).__init__(logger, name)
        self.clear()
        self.handler = ForwardingLogHandler(self.handle)

    def start_capture(self):
        """
        Attaches the adapter's handler to the adapted logger in order to start
        capturing log messages.
        """
        self.logger.addHandler(self.handler)

    def stop_capture(self):
        """
        Detaches the adapter's handler from the adapted logger. This should be
        called to prevent further logging to the adapted logger (possibly via
        other log adapter instances) being captured by this instance.
        """
        self.logger.removeHandler(self.handler)


@contextlib.contextmanager
def capture_logger(conf, *args, **kwargs):
    """
    Yields an adapted system logger based on the conf options. The log adapter
    captures logs in order to support the pattern of tests calling the log
    accessor methods (e.g. get_lines_for_level) directly on the logger
    instance.
    """
    with mock.patch('swift.common.utils.LogAdapter', CaptureLogAdapter):
        log_adapter = utils.get_logger(conf, *args, **kwargs)
    log_adapter.start_capture()
    try:
        yield log_adapter
    finally:
        log_adapter.stop_capture()
