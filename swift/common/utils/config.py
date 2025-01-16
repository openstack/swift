# Copyright (c) 2010-2012 OpenStack Foundation
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

import os
import operator
import re
import configparser
from configparser import ConfigParser, RawConfigParser

# Used when reading config values
TRUE_VALUES = {'true', '1', 'yes', 'on', 't', 'y'}


def config_true_value(value):
    """
    Returns True if the value is either True or a string in TRUE_VALUES.
    Returns False otherwise.
    """
    return value is True or \
        (isinstance(value, str) and value.lower() in TRUE_VALUES)


def _non_negative_number(value, expected_type_f=float,
                         expected_type_description='float number'):
    try:
        value = expected_type_f(value)
        if value < 0:
            raise ValueError
    except (TypeError, ValueError):
        raise ValueError('Value must be a non-negative %s, not "%s".'
                         % (expected_type_description, value))
    return value


def non_negative_float(value):
    """
    Check that the value casts to a float and is non-negative.

    :param value: value to check
    :raises ValueError: if the value cannot be cast to a float or is negative.
    :return: a float
    """
    return _non_negative_number(value)


def non_negative_int(value):
    """
    Check that the value casts to an int and is a whole number.

    :param value: value to check
    :raises ValueError: if the value cannot be cast to an int or does not
        represent a whole number.
    :return: an int
    """
    return _non_negative_number(value, expected_type_f=int,
                                expected_type_description='integer')


def config_positive_int_value(value):
    """
    Returns positive int value if it can be cast by int() and it's an
    integer > 0. (not including zero) Raises ValueError otherwise.
    """
    try:
        result = int(value)
        if result < 1:
            raise ValueError()
    except (TypeError, ValueError):
        raise ValueError(
            'Config option must be an positive int number, not "%s".' % value)
    return result


def config_float_value(value, minimum=None, maximum=None):
    try:
        val = float(value)
        if minimum is not None and val < minimum:
            raise ValueError()
        if maximum is not None and val > maximum:
            raise ValueError()
        return val
    except (TypeError, ValueError):
        min_ = ', greater than %s' % minimum if minimum is not None else ''
        max_ = ', less than %s' % maximum if maximum is not None else ''
        raise ValueError('Config option must be a number%s%s, not "%s".' %
                         (min_, max_, value))


def config_auto_int_value(value, default):
    """
    Returns default if value is None or 'auto'.
    Returns value as an int or raises ValueError otherwise.
    """
    if value is None or \
       (isinstance(value, str) and value.lower() == 'auto'):
        return default
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise ValueError('Config option must be an integer or the '
                         'string "auto", not "%s".' % value)
    return value


def config_percent_value(value):
    try:
        return config_float_value(value, 0, 100) / 100.0
    except ValueError as err:
        raise ValueError("%s: %s" % (str(err), value))


def config_request_node_count_value(value):
    try:
        value_parts = value.lower().split()
        rnc_value = int(value_parts[0])
    except (ValueError, AttributeError):
        pass
    else:
        if len(value_parts) == 1:
            return lambda replicas: rnc_value
        elif (len(value_parts) == 3 and
              value_parts[1] == '*' and
              value_parts[2] == 'replicas'):
            return lambda replicas: rnc_value * replicas
    raise ValueError(
        'Invalid request_node_count value: %r' % value)


def config_fallocate_value(reserve_value):
    """
    Returns fallocate reserve_value as an int or float.
    Returns is_percent as a boolean.
    Returns a ValueError on invalid fallocate value.
    """
    try:
        if str(reserve_value[-1:]) == '%':
            reserve_value = float(reserve_value[:-1])
            is_percent = True
        else:
            reserve_value = int(reserve_value)
            is_percent = False
    except ValueError:
        raise ValueError('Error: %s is an invalid value for fallocate'
                         '_reserve.' % reserve_value)
    return reserve_value, is_percent


def config_read_prefixed_options(conf, prefix_name, defaults):
    """
    Read prefixed options from configuration

    :param conf: the configuration
    :param prefix_name: the prefix (including, if needed, an underscore)
    :param defaults: a dict of default values. The dict supplies the
                     option name and type (string or comma separated string)
    :return: a dict containing the options
    """
    params = {}
    for option_name in defaults.keys():
        value = conf.get('%s%s' % (prefix_name, option_name))
        if value:
            if isinstance(defaults.get(option_name), list):
                params[option_name] = []
                for role in value.lower().split(','):
                    params[option_name].append(role.strip())
            else:
                params[option_name] = value.strip()
    return params


def append_underscore(prefix):
    if prefix and not prefix.endswith('_'):
        prefix += '_'
    return prefix


def config_read_reseller_options(conf, defaults):
    """
    Read reseller_prefix option and associated options from configuration

    Reads the reseller_prefix option, then reads options that may be
    associated with a specific reseller prefix. Reads options such that an
    option without a prefix applies to all reseller prefixes unless an option
    has an explicit prefix.

    :param conf: the configuration
    :param defaults: a dict of default values. The key is the option
                     name. The value is either an array of strings or a string
    :return: tuple of an array of reseller prefixes and a dict of option values
    """
    reseller_prefix_opt = conf.get('reseller_prefix', 'AUTH').split(',')
    reseller_prefixes = []
    for prefix in [pre.strip() for pre in reseller_prefix_opt if pre.strip()]:
        if prefix == "''":
            prefix = ''
        prefix = append_underscore(prefix)
        if prefix not in reseller_prefixes:
            reseller_prefixes.append(prefix)
    if len(reseller_prefixes) == 0:
        reseller_prefixes.append('')

    # Get prefix-using config options
    associated_options = {}
    for prefix in reseller_prefixes:
        associated_options[prefix] = dict(defaults)
        associated_options[prefix].update(
            config_read_prefixed_options(conf, '', defaults))
        prefix_name = prefix if prefix != '' else "''"
        associated_options[prefix].update(
            config_read_prefixed_options(conf, prefix_name, defaults))
    return reseller_prefixes, associated_options


def affinity_key_function(affinity_str):
    """Turns an affinity config value into a function suitable for passing to
    sort(). After doing so, the array will be sorted with respect to the given
    ordering.

    For example, if affinity_str is "r1=1, r2z7=2, r2z8=2", then the array
    will be sorted with all nodes from region 1 (r1=1) first, then all the
    nodes from region 2 zones 7 and 8 (r2z7=2 and r2z8=2), then everything
    else.

    Note that the order of the pieces of affinity_str is irrelevant; the
    priority values are what comes after the equals sign.

    If affinity_str is empty or all whitespace, then the resulting function
    will not alter the ordering of the nodes.

    :param affinity_str: affinity config value, e.g. "r1z2=3"
                         or "r1=1, r2z1=2, r2z2=2"
    :returns: single-argument function
    :raises ValueError: if argument invalid
    """
    affinity_str = affinity_str.strip()

    if not affinity_str:
        return lambda x: 0

    priority_matchers = []
    pieces = [s.strip() for s in affinity_str.split(',')]
    for piece in pieces:
        # matches r<number>=<number> or r<number>z<number>=<number>
        match = re.match(r"r(\d+)(?:z(\d+))?=(\d+)$", piece)
        if match:
            region, zone, priority = match.groups()
            region = int(region)
            priority = int(priority)
            zone = int(zone) if zone else None

            matcher = {'region': region, 'priority': priority}
            if zone is not None:
                matcher['zone'] = zone
            priority_matchers.append(matcher)
        else:
            raise ValueError("Invalid affinity value: %r" % affinity_str)

    priority_matchers.sort(key=operator.itemgetter('priority'))

    def keyfn(ring_node):
        for matcher in priority_matchers:
            if (matcher['region'] == ring_node['region']
                and ('zone' not in matcher
                     or matcher['zone'] == ring_node['zone'])):
                return matcher['priority']
        return 4294967296  # 2^32, i.e. "a big number"
    return keyfn


def affinity_locality_predicate(write_affinity_str):
    """
    Turns a write-affinity config value into a predicate function for nodes.
    The returned value will be a 1-arg function that takes a node dictionary
    and returns a true value if it is "local" and a false value otherwise. The
    definition of "local" comes from the affinity_str argument passed in here.

    For example, if affinity_str is "r1, r2z2", then only nodes where region=1
    or where (region=2 and zone=2) are considered local.

    If affinity_str is empty or all whitespace, then the resulting function
    will consider everything local

    :param write_affinity_str: affinity config value, e.g. "r1z2"
        or "r1, r2z1, r2z2"
    :returns: single-argument function, or None if affinity_str is empty
    :raises ValueError: if argument invalid
    """
    affinity_str = write_affinity_str.strip()

    if not affinity_str:
        return None

    matchers = []
    pieces = [s.strip() for s in affinity_str.split(',')]
    for piece in pieces:
        # matches r<number> or r<number>z<number>
        match = re.match(r"r(\d+)(?:z(\d+))?$", piece)
        if match:
            region, zone = match.groups()
            region = int(region)
            zone = int(zone) if zone else None

            matcher = {'region': region}
            if zone is not None:
                matcher['zone'] = zone
            matchers.append(matcher)
        else:
            raise ValueError("Invalid write-affinity value: %r" % affinity_str)

    def is_local(ring_node):
        for matcher in matchers:
            if (matcher['region'] == ring_node['region']
                and ('zone' not in matcher
                     or matcher['zone'] == ring_node['zone'])):
                return True
        return False
    return is_local


def read_conf_dir(parser, conf_dir):
    conf_files = []
    for f in os.listdir(conf_dir):
        if f.endswith('.conf') and not f.startswith('.'):
            conf_files.append(os.path.join(conf_dir, f))
    return parser.read(sorted(conf_files))


class NicerInterpolation(configparser.BasicInterpolation):
    def before_get(self, parser, section, option, value, defaults):
        if '%(' not in value:
            return value
        return super(NicerInterpolation, self).before_get(
            parser, section, option, value, defaults)


def readconf(conf_path, section_name=None, log_name=None, defaults=None,
             raw=False):
    """
    Read config file(s) and return config items as a dict

    :param conf_path: path to config file/directory, or a file-like object
                     (hasattr readline)
    :param section_name: config section to read (will return all sections if
                     not defined)
    :param log_name: name to be used with logging (will use section_name if
                     not defined)
    :param defaults: dict of default values to pre-populate the config with
    :returns: dict of config items
    :raises ValueError: if section_name does not exist
    :raises IOError: if reading the file failed
    """
    if defaults is None:
        defaults = {}
    if raw:
        c = RawConfigParser(defaults)
    else:
        # In general, we haven't really thought much about interpolation
        # in configs. Python's default ConfigParser has always supported
        # it, though, so *we* got it "for free". Unfortunatley, since we
        # "supported" interpolation, we have to assume there are
        # deployments in the wild that use it, and try not to break them.
        # So, do what we can to mimic the py2 behavior of passing through
        # values like "1%" (which we want to support for
        # fallocate_reserve).
        c = ConfigParser(defaults, interpolation=NicerInterpolation())
    c.optionxform = str  # Don't lower-case keys

    if hasattr(conf_path, 'readline'):
        if hasattr(conf_path, 'seek'):
            conf_path.seek(0)
        c.read_file(conf_path)
    else:
        if os.path.isdir(conf_path):
            # read all configs in directory
            success = read_conf_dir(c, conf_path)
        else:
            success = c.read(conf_path)
        if not success:
            raise IOError("Unable to read config from %s" %
                          conf_path)
    if section_name:
        if c.has_section(section_name):
            conf = dict(c.items(section_name))
        else:
            raise ValueError(
                "Unable to find %(section)s config section in %(conf)s" %
                {'section': section_name, 'conf': conf_path})
        if "log_name" not in conf:
            if log_name is not None:
                conf['log_name'] = log_name
            else:
                conf['log_name'] = section_name
    else:
        conf = {}
        for s in c.sections():
            conf.update({s: dict(c.items(s))})
        if 'log_name' not in conf:
            conf['log_name'] = log_name
    conf['__file__'] = conf_path
    return conf


def parse_prefixed_conf(conf_file, prefix):
    """
    Search the config file for any common-prefix sections and load those
    sections to a dict mapping the after-prefix reference to options.

    :param conf_file: the file name of the config to parse
    :param prefix: the common prefix of the sections
    :return: a dict mapping policy reference -> dict of policy options
    :raises ValueError: if a policy config section has an invalid name
    """

    ret_config = {}
    all_conf = readconf(conf_file)
    for section, options in all_conf.items():
        if not section.startswith(prefix):
            continue
        target_ref = section[len(prefix):]
        ret_config[target_ref] = options
    return ret_config
