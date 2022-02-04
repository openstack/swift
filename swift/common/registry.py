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

# Used by get_swift_info and register_swift_info to store information about
# the swift cluster.
from copy import deepcopy

_swift_info = {}
_swift_admin_info = {}


def get_swift_info(admin=False, disallowed_sections=None):
    """
    Returns information about the swift cluster that has been previously
    registered with the register_swift_info call.

    :param admin: boolean value, if True will additionally return an 'admin'
                  section with information previously registered as admin
                  info.
    :param disallowed_sections: list of section names to be withheld from the
                                information returned.
    :returns: dictionary of information about the swift cluster.
    """
    disallowed_sections = disallowed_sections or []
    info = deepcopy(_swift_info)
    for section in disallowed_sections:
        key_to_pop = None
        sub_section_dict = info
        for sub_section in section.split('.'):
            if key_to_pop:
                sub_section_dict = sub_section_dict.get(key_to_pop, {})
                if not isinstance(sub_section_dict, dict):
                    sub_section_dict = {}
                    break
            key_to_pop = sub_section
        sub_section_dict.pop(key_to_pop, None)

    if admin:
        info['admin'] = dict(_swift_admin_info)
        info['admin']['disallowed_sections'] = list(disallowed_sections)
    return info


def register_swift_info(name='swift', admin=False, **kwargs):
    """
    Registers information about the swift cluster to be retrieved with calls
    to get_swift_info.

    NOTE: Do not use "." in the param: name or any keys in kwargs. "." is used
          in the disallowed_sections to remove unwanted keys from /info.

    :param name: string, the section name to place the information under.
    :param admin: boolean, if True, information will be registered to an
                  admin section which can optionally be withheld when
                  requesting the information.
    :param kwargs: key value arguments representing the information to be
                   added.
    :raises ValueError: if name or any of the keys in kwargs has "." in it
    """
    if name == 'admin' or name == 'disallowed_sections':
        raise ValueError('\'{0}\' is reserved name.'.format(name))

    if admin:
        dict_to_use = _swift_admin_info
    else:
        dict_to_use = _swift_info
    if name not in dict_to_use:
        if "." in name:
            raise ValueError('Cannot use "." in a swift_info key: %s' % name)
        dict_to_use[name] = {}
    for key, val in kwargs.items():
        if "." in key:
            raise ValueError('Cannot use "." in a swift_info key: %s' % key)
        dict_to_use[name][key] = val
