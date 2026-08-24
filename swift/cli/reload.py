# Copyright (c) 2022 NVIDIA
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
Safely reload WSGI servers while minimizing client downtime and errors by

   * validating that the process is a Swift WSGI server manager,
   * checking that the configuration file used is valid,
   * sending the "seamless reload" signal, and
   * waiting for the reload to complete.
"""

import argparse
import errno
import os
import os.path
import socket
import subprocess
import sys

from swift.common.utils import NotificationServer
from swift.common.concurrency import signal_for
from swift.common.manager import pid_uses_eventlet


EXIT_BAD_PID = 2  # similar to argparse exiting 2 on an unknown arg
EXIT_RELOAD_FAILED = 1
EXIT_RELOAD_TIMEOUT = 128 + errno.ETIMEDOUT


def validate_manager_pid(pid):
    try:
        with open('/proc/%d/cmdline' % pid, 'r') as fp:
            cmd = fp.read().strip('\x00').split('\x00')
        sid = os.getsid(pid)
    except (IOError, OSError):
        print("Failed to get process information for %s" % pid,
              file=sys.stderr)
        exit(EXIT_BAD_PID)

    scripts = [os.path.basename(c) for c in cmd
               if '/bin/' in c and '/bin/python' not in c]

    if len(scripts) != 1 or not scripts[0].startswith("swift-"):
        print("Non-swift process: %r" % ' '.join(cmd), file=sys.stderr)
        exit(EXIT_BAD_PID)

    if scripts[0] not in {"swift-proxy-server", "swift-account-server",
                          "swift-container-server", "swift-object-server"}:
        print("Process does not support config checks: %s" % scripts[0],
              file=sys.stderr)
        exit(EXIT_BAD_PID)

    if sid != pid:
        print("Process appears to be a %s worker, not a manager. "
              "Did you mean %s?" % (scripts[0], sid), file=sys.stderr)
        exit(EXIT_BAD_PID)

    return cmd, scripts[0]


def _status(fields):
    for field in fields:
        if field.startswith(b"STATUS="):
            return field[len(b"STATUS="):].decode("utf8", "replace")
    return "no reason given"


def main(args=None):
    parser = argparse.ArgumentParser(__doc__)
    parser.add_argument("pid", type=int,
                        help="server PID which should be reloaded")
    wait_group = parser.add_mutually_exclusive_group()
    wait_group.add_argument("-t", "--timeout", type=float, default=300.0,
                            help="max time to wait for reload to complete")
    wait_group.add_argument("-w", "--no-wait",
                            action="store_false", dest="wait",
                            help="skip waiting for reload to complete")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="display more information as the process reloads")
    args = parser.parse_args(args)

    cmd, script = validate_manager_pid(args.pid)

    # Send the seamless-reload signal for the target's mode, not this
    # process's: gunicorn reads eventlet's SIGUSR1 as "reopen logs", not
    # reload.
    try:
        target = pid_uses_eventlet(args.pid)
    except OSError:
        # Truly unknown (unreadable environment). USR1 is still the safe
        # choice -- a gunicorn master only reopens its logs -- but say so.
        print('Warning: could not read the mode of pid %s; sending '
              'eventlet\'s USR1, which a gunicorn master treats as '
              '"reopen logs" -- it will NOT reload. Pin USE_EVENTLET in '
              'the server environment to avoid this.'
              % args.pid, file=sys.stderr)
        target = None
    if target is None:
        # Readable but unpinned: a server started before swift-init pinned
        # USE_EVENTLET on spawn -- legacy eventlet.
        target = True
    sig = signal_for('seamless', uses_eventlet=target)

    if args.verbose:
        print("Checking config for %s" % script)
    # --test-config picks its mode from USE_EVENTLET in its own env, so pin it
    # to the target's; else a gunicorn server is validated via the eventlet
    # path and gunicorn-only config errors slip through, crashing the master
    # on SIGHUP.
    env = dict(os.environ)
    env['USE_EVENTLET'] = 'true' if target else 'false'
    # The target is a session leader (validate_manager_pid required sid==pid),
    # so its pid is its workers' sid. Pass it so bind validation tolerates an
    # in-use port only for this server's own listener, not another process on
    # a newly configured port.
    env['SWIFT_RELOAD_OWNER_SID'] = str(args.pid)
    try:
        subprocess.check_call(cmd + ["--test-config"], env=env)
    except subprocess.CalledProcessError:
        print("Failed to validate config", file=sys.stderr)
        exit(EXIT_RELOAD_FAILED)

    if args.wait:
        try:
            with NotificationServer(args.pid, args.timeout) as notifications:
                if args.verbose:
                    print("Sending %s signal" % sig.name)
                os.kill(args.pid, sig)

                try:
                    ready = False
                    while not ready:
                        fields = notifications.receive().split(b"\n")
                        for data in fields:
                            if args.verbose:
                                if data in (b"READY=1", b"RELOADING=1",
                                            b"STOPPING=1"):
                                    print("Process is %s" %
                                          data.decode("ascii")[:-2])
                                else:
                                    print("Received notification %r" % data)

                        if any(f.startswith(b"ERRNO=") for f in fields):
                            # the server kept running on its old config, so
                            # no readiness is coming
                            print("Failed to reload %s: %s"
                                  % (script, _status(fields)), file=sys.stderr)
                            exit(EXIT_RELOAD_FAILED)
                        if b"READY=1" in fields:
                            ready = True
                except socket.timeout:
                    print("Timed out reloading %s" % script, file=sys.stderr)
                    exit(EXIT_RELOAD_TIMEOUT)
        except OSError as e:
            print("Could not bind notification socket: %s" % e,
                  file=sys.stderr)
            exit(EXIT_RELOAD_FAILED)
    else:  # --no-wait
        if args.verbose:
            print("Sending %s signal" % sig.name)
        os.kill(args.pid, sig)

    print("Reloaded %s" % script)


if __name__ == "__main__":
    main()
