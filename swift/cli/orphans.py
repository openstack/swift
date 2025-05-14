#!/usr/bin/env python
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

import optparse
import os
import re
import signal
import subprocess
import sys

from swift.common.manager import RUN_DIR


def main():
    parser = optparse.OptionParser(usage='''%prog [options]

Lists and optionally kills orphaned Swift processes. This is done by scanning
/var/run/swift for .pid files and listing any processes that look like Swift
processes but aren't associated with the pids in those .pid files. Any Swift
processes running with the 'once' parameter are ignored, as those are usually
for full-speed audit scans and such.

Example (sends SIGTERM to all orphaned Swift processes older than two hours):
%prog -a 2 -k TERM
        '''.strip())
    parser.add_option('-a', '--age', dest='hours', type='int', default=24,
                      help="look for processes at least HOURS old; "
                      "default: 24")
    parser.add_option('-k', '--kill', dest='signal',
                      help='send SIGNAL to matched processes; default: just '
                      'list process information')
    parser.add_option('-w', '--wide', dest='wide', default=False,
                      action='store_true',
                      help="don't clip the listing at 80 characters")
    parser.add_option('-r', '--run-dir', type="str",
                      dest="run_dir", default=RUN_DIR,
                      help="alternative directory to store running pid files "
                      "default: %s" % RUN_DIR)
    (options, args) = parser.parse_args()

    pids = []

    for root, directories, files in os.walk(options.run_dir):
        for name in files:
            if name.endswith(('.pid', '.pid.d')):
                pids.append(open(os.path.join(root, name)).read().strip())
                pids.extend(subprocess.Popen(
                    ['ps', '--ppid', pids[-1], '-o', 'pid', '--no-headers'],
                    stdout=subprocess.PIPE).communicate()[0].decode().split())

    listing = []
    swift_cmd_re = re.compile(
        '^/usr/bin/python[23]? /usr(?:/local)?/bin/swift-')
    for line in subprocess.Popen(
            ['ps', '-eo', 'etime,pid,args', '--no-headers'],
            stdout=subprocess.PIPE).communicate()[0].split(b'\n'):
        if not line:
            continue
        hours = 0
        try:
            etime, pid, args = line.decode('ascii').split(None, 2)
        except ValueError:
            sys.exit('Could not process ps line %r' % line)
        if pid in pids:
            continue
        if any([
            not swift_cmd_re.match(args),
            'swift-orphans' in args,
            'once' in args.split(),
        ]):
            continue
        args = args.split('swift-', 1)[1]
        etime = etime.split('-')
        if len(etime) == 2:
            hours = int(etime[0]) * 24
            etime = etime[1]
        elif len(etime) == 1:
            etime = etime[0]
        else:
            sys.exit('Could not process etime value from %r' % line)
        etime = etime.split(':')
        if len(etime) == 3:
            hours += int(etime[0])
        elif len(etime) != 2:
            sys.exit('Could not process etime value from %r' % line)
        if hours >= options.hours:
            listing.append((str(hours), pid, args))

    if not listing:
        sys.exit()

    hours_len = len('Hours')
    pid_len = len('PID')
    args_len = len('Command')
    for hours, pid, args in listing:
        hours_len = max(hours_len, len(hours))
        pid_len = max(pid_len, len(pid))
        args_len = max(args_len, len(args))
    args_len = min(args_len, 78 - hours_len - pid_len)

    print('%*s %*s %s' %
          (hours_len, 'Hours', pid_len, 'PID', 'Command'))
    for hours, pid, args in listing:
        print('%*s %*s %s' %
              (hours_len, hours, pid_len, pid, args[:args_len]))

    if options.signal:
        try:
            signum = int(options.signal)
        except ValueError:
            signum = getattr(signal, options.signal.upper(),
                             getattr(signal, 'SIG' + options.signal.upper(),
                                     None))
        if not signum:
            sys.exit('Could not translate %r to a signal number.' %
                     options.signal)
        print('Sending processes %s (%d) signal...' % (options.signal, signum),
              end='')
        for hours, pid, args in listing:
            os.kill(int(pid), signum)
        print('Done.')


if __name__ == '__main__':
    main()
