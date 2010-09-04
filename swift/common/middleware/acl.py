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

def clean_acl(name, value):
    values = []
    for raw_value in value.lower().split(','):
        raw_value = raw_value.strip()
        if raw_value:
            if ':' in raw_value:
                first, second = (v.strip() for v in raw_value.split(':', 1))
                if not first:
                    raise ValueError('No value before colon in %s' %
                                     repr(raw_value))
                if first == '.ref' and 'write' in name:
                    raise ValueError('Referrers not allowed in write ACLs: %s'
                                     % repr(raw_value))
                if second:
                    if first == '.ref' and second[0] == '-':
                        second = second[1:].strip()
                        if not second:
                            raise ValueError('No value after referrer deny '
                                'designation in %s' % repr(raw_value))
                        second = '-' + second
                    values.append('%s:%s' % (first, second))
                elif first == '.ref':
                    raise ValueError('No value after referrer designation in '
                                     '%s' % repr(raw_value))
                else:
                    values.append(first)
            else:
                values.append(raw_value)
    return ','.join(values)


def parse_acl(acl_string):
    referrers = []
    groups = []
    if acl_string:
        for value in acl_string.split(','):
            if value.startswith('.ref:'):
                referrers.append(value[len('.ref:'):])
            else:
                groups.append(value)
    return referrers, groups


def referrer_allowed(req, referrers):
    allow = False
    if referrers:
        parts = req.referer.split('//', 1)
        if len(parts) == 2:
            rhost = parts[1].split('/', 1)[0].split(':', 1)[0].lower()
        else:
            rhost = 'unknown'
        for mhost in referrers:
            if mhost[0] == '-':
                mhost = mhost[1:]
                if mhost == rhost or \
                       (mhost[0] == '.' and rhost.endswith(mhost)):
                    allow = False
            elif mhost == 'any' or mhost == rhost or \
                    (mhost[0] == '.' and rhost.endswith(mhost)):
                allow = True
    return allow
