#!/usr/bin/python
# Copyright (c) 2010 OpenStack, LLC.
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

import sys
import urllib

from swift.common.ring import Ring
from swift.common.utils import hash_path


if len(sys.argv) < 3 or len(sys.argv) > 5:
    print 'Usage: %s <ring.gz> <account> [<container>] [<object>]' % sys.argv[0]
    print 'Shows the nodes responsible for the item specified.'
    print 'Example:'
    print '    $ %s /etc/swift/account.ring.gz MyAccount' % sys.argv[0]
    print '    Partition 5743883'
    print '    Hash 96ae332a60b58910784e4417a03e1ad0'
    print '    10.1.1.7:8000 sdd1'
    print '    10.1.9.2:8000 sdb1'
    print '    10.1.5.5:8000 sdf1'
    sys.exit(1)

ringloc = None
account = None
container = None
obj = None

if len(sys.argv) > 4: ring,account,container,obj = sys.argv[1:5]
elif len(sys.argv) > 3: ring,account,container = sys.argv[1:4]
elif len(sys.argv) > 2: ring,account = sys.argv[1:3]

print '\nAccount \t%s' % account
print 'Container\t%s' % container
print 'Object   \t%s\n' % obj

if obj:
    hash_str = hash_path(account,container,obj)
    part, nodes = Ring(ring).get_nodes(account,container,obj)
    for node in nodes:
        print 'Server:Port Device\t%s:%s %s' % (node['ip'], node['port'], node['device'])
    print '\nPartition\t%s' % part
    print 'Hash        \t%s\n' % hash_str
    for node in nodes:
      acct_cont_obj = "%s/%s/%s" % (account, container, obj)
      print 'curl -I -XHEAD "http://%s:%s/%s/%s/%s"' % (node['ip'],node['port'],node['device'],part,urllib.quote(acct_cont_obj))
    print "\n"
    for node in nodes:
      print 'ssh %s "ls -lah /srv/node/%s/objects/%s/%s/%s/"' % (node['ip'],node['device'],part,hash_str[-3:],hash_str)
elif container:
    hash_str = hash_path(account,container)
    part, nodes = Ring(ring).get_nodes(account,container)
    for node in nodes:
        print 'Server:Port Device\t%s:%s %s' % (node['ip'], node['port'], node['device'])
    print '\nPartition %s' % part
    print 'Hash %s\n' % hash_str
    for node in nodes:
      acct_cont = "%s/%s" % (account,container)
      print 'curl -I -XHEAD "http://%s:%s/%s/%s/%s"' % (node['ip'],node['port'],node['device'],part,urllib.quote(acct_cont))
    print "\n"
    for node in nodes:
      print 'ssh %s "ls -lah /srv/node/%s/containers/%s/%s/%s/%s.db"' % (node['ip'],node['device'],part,hash_str[-3:],hash_str,hash_str)
elif account:
    hash_str = hash_path(account)
    part, nodes = Ring(ring).get_nodes(account)
    for node in nodes:
        print 'Server:Port Device\t%s:%s %s' % (node['ip'], node['port'], node['device'])
    print '\nPartition %s' % part
    print 'Hash %s\n' % hash_str
    for node in nodes:
      print 'curl -I -XHEAD "http://%s:%s/%s/%s/%s"' % (node['ip'],node['port'],node['device'],part, urllib.quote(account))
    print "\n"
    for node in nodes:
      print 'ssh %s "ls -lah /srv/node/%s/accounts/%s/%s/%s/%s.db"' % (node['ip'],node['device'],part,hash_str[-3:],hash_str,hash_str)
    print "\n\n"
