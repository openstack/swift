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

from __future__ import print_function
import argparse
import errno
import os
import os.path
import signal
import subprocess
import sys
import time

from swift.common.manager import get_child_pids


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

    if args.verbose:
        print("Checking config for %s" % script)
    try:
        subprocess.check_call(cmd + ["--test-config"])
    except subprocess.CalledProcessError:
        print("Failed to validate config", file=sys.stderr)
        exit(EXIT_RELOAD_FAILED)

    if args.wait:
        try:
            original_children = get_child_pids(args.pid)
            children_since_reload = set()

            if args.verbose:
                print("Sending USR1 signal")
            os.kill(args.pid, signal.SIGUSR1)

            start = time.time()
            while time.time() - start < args.timeout:
                children = get_child_pids(args.pid)
                new_children = (children - original_children
                                - children_since_reload)
                if new_children:
                    if args.verbose:
                        print("Found new children: %s" % ", ".join(
                            str(pid) for pid in new_children))
                    children_since_reload |= new_children
                if children_since_reload - children:
                    # At least one new child exited; presumably, it was
                    # the temporary child waiting to shutdown sockets
                    break
                # We want this to be fairly low, since the temporary child
                # may not hang around very long
                time.sleep(0.1)
            else:
                print("Timed out reloading %s" % script, file=sys.stderr)
                exit(EXIT_RELOAD_TIMEOUT)

        except subprocess.CalledProcessError:
            # This could pop during any of the calls to get_child_pids
            print("Process seems to have died!", file=sys.stderr)
            exit(EXIT_RELOAD_FAILED)
    else:  # --no-wait
        if args.verbose:
            print("Sending USR1 signal")
        os.kill(args.pid, signal.SIGUSR1)

    print("Reloaded %s" % script)


if __name__ == "__main__":
    main()
