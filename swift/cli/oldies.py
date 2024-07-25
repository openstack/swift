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
import subprocess
import sys


def main():
    parser = optparse.OptionParser(usage='''%prog [options]

Lists old Swift processes.
        '''.strip())
    parser.add_option('-a', '--age', dest='hours', type='int', default=720,
                      help='look for processes at least HOURS old; '
                      'default: 720 (30 days)')
    parser.add_option('-p', '--pids', action='store_true',
                      help='only print the pids found; for example, to pipe '
                      'to xargs kill')
    (options, args) = parser.parse_args()

    listing = []
    for line in subprocess.Popen(
            ['ps', '-eo', 'etime,pid,args', '--no-headers'],
            stdout=subprocess.PIPE).communicate()[0].split(b'\n'):
        if not line:
            continue
        hours = 0
        try:
            etime, pid, args = line.decode('ascii').split(None, 2)
        except ValueError:
            # This covers both decoding and not-enough-values-to-unpack errors
            sys.exit('Could not process ps line %r' % line)
        if not args.startswith((
                '/usr/bin/python /usr/bin/swift-',
                '/usr/bin/python /usr/local/bin/swift-',
                '/bin/python /usr/bin/swift-',
                '/usr/bin/python3 /usr/bin/swift-',
                '/usr/bin/python3 /usr/local/bin/swift-',
                '/bin/python3 /usr/bin/swift-')):
            continue
        args = args.split('-', 1)[1]
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

    if options.pids:
        for hours, pid, args in listing:
            print(pid)
    else:
        hours_len = len('Hours')
        pid_len = len('PID')
        args_len = len('Command')
        for hours, pid, args in listing:
            hours_len = max(hours_len, len(hours))
            pid_len = max(pid_len, len(pid))
            args_len = max(args_len, len(args))
        args_len = min(args_len, 78 - hours_len - pid_len)

        print('%*s %*s %s' % (hours_len, 'Hours', pid_len, 'PID', 'Command'))
        for hours, pid, args in listing:
            print('%*s %*s %s' % (hours_len, hours, pid_len,
                                  pid, args[:args_len]))


if __name__ == '__main__':
    main()
