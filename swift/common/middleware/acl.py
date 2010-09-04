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
    """
    Returns a cleaned ACL header value, validating that it meets the formatting
    requirements for standard Swift ACL strings.

    The ACL format is::

        [item[,item...]]

    Each item can be a group name to give access to or a referrer designation
    to grant or deny based on the HTTP Referer header.

    The referrer designation format is::

        .ref:[-]value

    The value can be "any" to specify any referrer host is allowed access, a
    specific host name like "www.example.com", or if it has a leading period
    "." it is a domain name specification, like ".example.com". The leading
    minus sign "-" indicates referrer hosts that should be denied access.

    Referrer access is applied in the order they are specified. For example,
    .ref:.example.com,.ref:-thief.example.com would allow all hosts ending with
    .example.com except for the specific host thief.example.com.

    Example valid ACLs::

        .ref:any
        .ref:any,.ref:-.thief.com
        .ref:any,.ref:-.thief.com,bobs_account,sues_account:sue
        bobs_account,sues_account:sue

    Example invalid ACLs::

        .ref:
        .ref:-

    Also, .ref designations aren't allowed in headers whose names include the
    word 'write'.

    ACLs that are "messy" will be cleaned up. Examples:

    ======================  ======================
    Original                Cleaned
    ----------------------  ----------------------
    bob, sue                bob,sue
    bob , sue               bob,sue
    bob,,,sue               bob,sue
    .ref : any              .ref:any
    ======================  ======================

    :param name: The name of the header being cleaned, such as X-Container-Read
                 or X-Container-Write.
    :param value: The value of the header being cleaned.
    :returns: The value, cleaned of extraneous formatting.
    :raises ValueError: If the value does not meet the ACL formatting
                        requirements; the error message will indicate why.
    """
    values = []
    for raw_value in value.lower().split(','):
        raw_value = raw_value.strip()
        if raw_value:
            if ':' not in raw_value:
                values.append(raw_value)
            else:
                first, second = (v.strip() for v in raw_value.split(':', 1))
                if first != '.ref':
                    values.append(raw_value)
                elif 'write' in name:
                    raise ValueError('Referrers not allowed in write ACLs: %s'
                                     % repr(raw_value))
                elif not second:
                    raise ValueError('No value after referrer designation in '
                                     '%s' % repr(raw_value))
                else:
                    if second[0] == '-':
                        second = second[1:].strip()
                        if not second:
                            raise ValueError('No value after referrer deny '
                                'designation in %s' % repr(raw_value))
                        second = '-' + second
                    values.append('%s:%s' % (first, second))
    return ','.join(values)


def parse_acl(acl_string):
    """
    Parses a standard Swift ACL string into a referrers list and groups list.

    See :func:`clean_acl` for documentation of the standard Swift ACL format.

    :param acl_string: The standard Swift ACL string to parse.
    :returns: A tuple of (referrers, groups) where referrers is a list of
              referrer designations (without the leading .ref:) and groups is a
              list of groups to allow access.
    """
    referrers = []
    groups = []
    if acl_string:
        for value in acl_string.split(','):
            if value.startswith('.ref:'):
                referrers.append(value[len('.ref:'):])
            else:
                groups.append(value)
    return referrers, groups


def referrer_allowed(referrer, referrer_acl):
    """
    Returns True if the referrer should be allowed based on the referrer_acl
    list (as returned by :func:`parse_acl`).

    See :func:`clean_acl` for documentation of the standard Swift ACL format.

    :param referrer: The value of the HTTP Referer header.
    :param referrer_acl: The list of referrer designations as returned by
                         :func:`parse_acl`.
    :returns: True if the referrer should be allowed; False if not.
    """
    allow = False
    if referrer_acl:
        if not referrer:
            rhost = 'unknown'
        else:
            parts = referrer.split('//', 1)
            if len(parts) == 2:
                rhost = parts[1].split('/', 1)[0].split(':', 1)[0].lower()
            else:
                rhost = 'unknown'
        for mhost in referrer_acl:
            if mhost[0] == '-':
                mhost = mhost[1:]
                if mhost == rhost or \
                       (mhost[0] == '.' and rhost.endswith(mhost)):
                    allow = False
            elif mhost == 'any' or mhost == rhost or \
                    (mhost[0] == '.' and rhost.endswith(mhost)):
                allow = True
    return allow
