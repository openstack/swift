# Copyright (c) 2017 OpenStack Foundation
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
``swift-ring-composer`` is an experimental tool for building a composite ring
file from other existing component ring builder files. Its CLI, name or
implementation may change or be removed altogether in future versions of Swift.

Currently its interface is similar to that of the ``swift-ring-builder``. The
command structure takes the form of::

    swift-ring-composer <composite builder file> <sub-command> <options>

where ``<composite builder file>`` is a special builder which stores a json
blob of composite ring metadata. This metadata describes the component
``RingBuilder``'s used in the composite ring, their order and version.

There are currently 2 sub-commands: ``show`` and ``compose``. The ``show``
sub-command takes no additional arguments and displays the current contents of
of the composite builder file::

    swift-ring-composer <composite builder file> show

The ``compose`` sub-command is the one that actually stitches the component
ring builders together to create both the composite ring file and composite
builder file. The command takes the form::

    swift-ring-composer <composite builder file> compose <builder1> \\
    <builder2> [<builder3> .. <builderN>] --output <composite ring file> \\
    [--force]

There may look like there is a lot going on there but it's actually quite
simple. The ``compose`` command takes in the list of builders to stitch
together and the filename for the composite ring file via the ``--output``
option. The ``--force`` option overrides checks on the ring composition.

To change ring devices, first add or remove devices from the component ring
builders and then use the ``compose`` sub-command to create a new composite
ring file.

.. note::

    ``swift-ring-builder`` cannot be used to inspect the generated composite
    ring file because there is no conventional builder file corresponding to
    the composite ring file name. You can either programmatically look inside
    the composite ring file using the swift ring classes or create a temporary
    builder file from the composite ring file using::

        swift-ring-builder <composite ring file> write_builder

    Do not use this builder file to manage ring devices.

For further details use::

  swift-ring-composer -h
"""
from __future__ import print_function
import argparse
import json
import os
import sys

from swift.common.ring.composite_builder import CompositeRingBuilder

EXIT_SUCCESS = 0
EXIT_ERROR = 2

WARNING = """
NOTE: This tool is for experimental use and may be
      removed in future versions of Swift.
"""

DESCRIPTION = """
This is a tool for building a composite ring file from other existing ring
builder files. The component ring builders must all have the same partition
power. Each device must only be used in a single component builder. Each region
must only be used in a single component builder.
"""


def _print_to_stderr(msg):
    print(msg, file=sys.stderr)


def _print_err(msg, err):
    _print_to_stderr('%s\nOriginal exception message:\n%s' % (msg, err))


def show(composite_builder, args):
    print(json.dumps(composite_builder.to_dict(), indent=4, sort_keys=True))
    return EXIT_SUCCESS


def compose(composite_builder, args):
    composite_builder = composite_builder or CompositeRingBuilder()
    try:
        ring_data = composite_builder.compose(
            args.builder_files, force=args.force, require_modified=True)
    except Exception as err:
        _print_err(
            'An error occurred while composing the ring.', err)
        return EXIT_ERROR
    try:
        ring_data.save(args.output)
    except Exception as err:
        _print_err(
            'An error occurred while writing the composite ring file.', err)
        return EXIT_ERROR
    try:
        composite_builder.save(args.composite_builder_file)
    except Exception as err:
        _print_err(
            'An error occurred while writing the composite builder file.', err)
        return EXIT_ERROR
    return EXIT_SUCCESS


def main(arguments=None):
    if arguments is not None:
        argv = arguments
    else:
        argv = sys.argv

    parser = argparse.ArgumentParser(description=DESCRIPTION)
    parser.add_argument(
        'composite_builder_file',
        metavar='composite_builder_file', type=str,
        help='Name of composite builder file')

    subparsers = parser.add_subparsers(
        help='subcommand help', title='subcommands')

    # show
    show_parser = subparsers.add_parser(
        'show', help='show composite ring builder metadata')
    show_parser.set_defaults(func=show)

    # compose
    compose_parser = subparsers.add_parser(
        'compose', help='compose composite ring',
        usage='%(prog)s [-h] '
              '[builder_file builder_file [builder_file ...] '
              '--output ring_file [--force]')
    bf_help = ('Paths to component ring builder files to include in composite '
               'ring')
    compose_parser.add_argument('builder_files', metavar='builder_file',
                                nargs='*', type=str, help=bf_help)
    compose_parser.add_argument('--output', metavar='output_file', type=str,
                                required=True, help='Name of output ring file')
    compose_parser.add_argument(
        '--force', action='store_true',
        help='Force new composite ring file to be written')
    compose_parser.set_defaults(func=compose)

    _print_to_stderr(WARNING)
    args = parser.parse_args(argv[1:])
    composite_builder = None
    if args.func != compose or os.path.exists(args.composite_builder_file):
        try:
            composite_builder = CompositeRingBuilder.load(
                args.composite_builder_file)
        except Exception as err:
            _print_err(
                'An error occurred while loading the composite builder file.',
                err)
            exit(EXIT_ERROR)

    exit(args.func(composite_builder, args))


if __name__ == '__main__':
    main()
