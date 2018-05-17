# Copyright (c) 2010-2017 OpenStack Foundation
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
A standard ring built using the :ref:`ring-builder <ring_builder>` will attempt
to randomly disperse replicas or erasure-coded fragments across failure
domains, but does not provide any guarantees such as placing at least one
replica of every partition into each region. Composite rings are intended to
provide operators with greater control over the dispersion of object replicas
or fragments across a cluster, in particular when there is a desire to
have strict guarantees that some replicas or fragments are placed in certain
failure domains. This is particularly important for policies with duplicated
erasure-coded fragments.

A composite ring comprises two or more component rings that are combined to
form a single ring with a replica count equal to the sum of replica counts
from the component rings. The component rings are built independently, using
distinct devices in distinct regions, which means that the dispersion of
replicas between the components can be guaranteed. The ``composite_builder``
utilities may then be used to combine components into a composite ring.

For example, consider a normal ring ``ring0`` with replica count of 4 and
devices in two regions ``r1`` and ``r2``. Despite the best efforts of the
ring-builder, it is possible for there to be three replicas of a particular
partition placed in one region and only one replica placed in the other region.
For example::

  part_n -> r1z1h110/sdb r1z2h12/sdb r1z3h13/sdb r2z1h21/sdb

Now consider two normal rings each with replica count of 2: ``ring1`` has
devices in only ``r1``; ``ring2`` has devices in only ``r2``.
When these rings are combined into a composite ring then every partition is
guaranteed to be mapped to two devices in each of ``r1`` and ``r2``, for
example::

  part_n -> r1z1h10/sdb r1z2h20/sdb  r2z1h21/sdb r2z2h22/sdb
            |_____________________|  |_____________________|
                       |                        |
                     ring1                    ring2

The dispersion of partition replicas across failure domains within each of the
two component rings may change as they are modified and rebalanced, but the
dispersion of replicas between the two regions is guaranteed by the use of a
composite ring.

For rings to be formed into a composite they must satisfy the following
requirements:

* All component rings must have the same part power (and therefore number of
  partitions)
* All component rings must have an integer replica count
* Each region may only be used in one component ring
* Each device may only be used in one component ring

Under the hood, the composite ring has a ``_replica2part2dev_id`` table that is
the union of the tables from the component rings. Whenever the component rings
are rebalanced, the composite ring must be rebuilt. There is no dynamic
rebuilding of the composite ring.

.. note::
    The order in which component rings are combined into a composite ring is
    very significant because it determines the order in which the
    Ring.get_part_nodes() method will provide primary nodes for the composite
    ring and consequently the node indexes assigned to the primary nodes. For
    an erasure-coded policy, inadvertent changes to the primary node indexes
    could result in large amounts of data movement due to fragments being moved
    to their new correct primary.

    The ``id`` of each component RingBuilder is therefore stored in metadata of
    the composite and used to check for the component ordering when the same
    composite ring is re-composed. RingBuilder ``id``\s are normally assigned
    when a RingBuilder instance is first saved. Older RingBuilder instances
    loaded from file may not have an ``id`` assigned and will need to be saved
    before they can be used as components of a composite ring. This can be
    achieved by, for example::

        swift-ring-builder <builder-file> rebalance --force

"""

import copy
import json
import os

from random import shuffle

from swift.common.exceptions import RingBuilderError
from swift.common.ring import RingBuilder
from swift.common.ring import RingData
from collections import defaultdict
from itertools import combinations

MUST_MATCH_ATTRS = (
    'part_power',
)


def pre_validate_all_builders(builders):
    """
    Pre-validation for all component ring builders that are to be included in
    the composite ring. Checks that all component rings are valid with respect
    to each other.

    :param builders: a list of :class:`swift.common.ring.builder.RingBuilder`
        instances
    :raises ValueError: if the builders are invalid with respect to each other
    """
    if len(builders) < 2:
        raise ValueError('Two or more component builders are required.')

    # all ring builders should be consistent for each MUST_MATCH_ATTRS
    for attr in MUST_MATCH_ATTRS:
        attr_dict = defaultdict(list)
        for i, builder in enumerate(builders):
            value = getattr(builder, attr, None)
            attr_dict[value].append(i)
        if len(attr_dict) > 1:
            variations = ['%s=%s found at indexes %s' %
                          (attr, val, indexes)
                          for val, indexes in attr_dict.items()]
            raise ValueError(
                'All builders must have same value for %r.\n%s'
                % (attr, '\n  '.join(variations)))

    # all ring builders should have int replica count and not have dirty mods
    errors = []
    for index, builder in enumerate(builders):
        if int(builder.replicas) != builder.replicas:
            errors.append(
                'Non integer replica count %s found at index %s' %
                (builder.replicas, index))
        if builder.devs_changed:
            errors.append(
                'Builder needs rebalance to apply changes at index %s' %
                index)
    if errors:
        raise ValueError(
            'Problem with builders.\n%s' % ('\n  '.join(errors)))

    # check regions
    regions_info = {}
    for builder in builders:
        regions_info[builder] = set(
            dev['region'] for dev in builder._iter_devs())
    for first_region_set, second_region_set in combinations(
            regions_info.values(), 2):
        inter = first_region_set & second_region_set
        if inter:
            raise ValueError('Same region found in different rings')

    # check device uniqueness
    check_for_dev_uniqueness(builders)


def check_for_dev_uniqueness(builders):
    """
    Check that no device appears in more than one of the given list of
    builders.

    :param builders: a list of :class:`swift.common.ring.builder.RingBuilder`
        instances
    :raises ValueError: if the same device is found in more than one builder
    """
    builder2devs = []
    for i, builder in enumerate(builders):
        dev_set = set()
        for dev in builder._iter_devs():
            ip, port, device = (dev['ip'], dev['port'], dev['device'])
            for j, (other_builder, devs) in enumerate(builder2devs):
                if (ip, port, device) in devs:
                    raise ValueError(
                        'Duplicate ip/port/device combination %s/%s/%s found '
                        'in builders at indexes %s and %s' %
                        (ip, port, device, j, i)
                    )
            dev_set.add((ip, port, device))
        builder2devs.append((builder, dev_set))


def _make_composite_ring(builders):
    """
    Given a list of component ring builders, return a composite RingData
    instance.

    :param builders: a list of
        :class:`swift.common.ring.builder.RingBuilder` instances
    :return: a new RingData instance built from the component builders
    :raises ValueError: if the builders are invalid with respect to each other
    """
    composite_r2p2d = []
    composite_devs = []
    device_offset = 0
    for builder in builders:
        # copy all devs list and replica2part2dev table to be able
        # to modify the id for each dev
        devs = copy.deepcopy(builder.devs)
        r2p2d = copy.deepcopy(builder._replica2part2dev)
        for part2dev in r2p2d:
            for part, dev in enumerate(part2dev):
                part2dev[part] += device_offset
        for dev in [d for d in devs if d]:
            # note that some devs may not be referenced in r2p2d but update
            # their dev id nonetheless
            dev['id'] += device_offset
        composite_r2p2d.extend(r2p2d)
        composite_devs.extend(devs)
        device_offset += len(builder.devs)

    return RingData(composite_r2p2d, composite_devs, builders[0].part_shift)


def compose_rings(builders):
    """
    Given a list of component ring builders, perform validation on the list of
    builders and return a composite RingData instance.

    :param builders: a list of
        :class:`swift.common.ring.builder.RingBuilder` instances
    :return: a new RingData instance built from the component builders
    :raises ValueError: if the builders are invalid with respect to each other
    """
    pre_validate_all_builders(builders)
    rd = _make_composite_ring(builders)
    return rd


def _make_component_meta(builder):
    """
    Return a dict of selected builder attributes to save in composite meta. The
    dict has keys ``version``, ``replicas`` and ``id``.
    :param builder: a :class:`swift.common.ring.builder.RingBuilder`
        instance
    :return: a dict of component metadata
    """
    attrs = ['version', 'replicas', 'id']
    metadata = dict((attr, getattr(builder, attr)) for attr in attrs)
    return metadata


def _make_composite_metadata(builders):
    """
    Return a dict with key ``components`` that maps to a list of dicts, each
    dict being of the form returned by :func:`_make_component_meta`.

    :param builders: a list of
        :class:`swift.common.ring.builder.RingBuilder` instances
    :return: a dict of composite metadata
    """
    component_meta = [_make_component_meta(builder) for builder in builders]
    return {'components': component_meta}


def check_same_builder(old_component, new_component):
    """
    Check that the given new_component metadata describes the same builder as
    the given old_component metadata. The new_component builder does not
    necessarily need to be in the same state as when the old_component metadata
    was created to satisfy this check e.g. it may have changed devs and been
    rebalanced.

    :param old_component: a dict of metadata describing a component builder
    :param new_component: a dict of metadata describing a component builder
    :raises ValueError: if the new_component is not the same as that described
        by the old_component
    """
    for key in ['replicas', 'id']:
        if old_component[key] != new_component[key]:
            raise ValueError("Attribute mismatch for %s: %r != %r" %
                             (key, old_component[key], new_component[key]))


def is_builder_newer(old_component, new_component):
    """
    Return True if the given builder has been modified with respect to its
    state when the given component_meta was created.

    :param old_component: a dict of metadata describing a component ring
    :param new_component: a dict of metadata describing a component ring
    :return: True if the builder has been modified, False otherwise.
    :raises ValueError: if the version of the new_component is older than the
                        version of the existing component.
    """

    if new_component['version'] < old_component['version']:
        raise ValueError('Older builder version: %s < %s' %
                         (new_component['version'], old_component['version']))
    return old_component['version'] < new_component['version']


def check_against_existing(old_composite_meta, new_composite_meta):
    """
    Check that the given builders and their order are the same as that
    used to build an existing composite ring. Return True if any of the given
    builders has been modified with respect to its state when the given
    component_meta was created.

    :param old_composite_meta: a dict of the form returned by
        :func:`_make_composite_meta`
    :param new_composite_meta: a dict of the form returned by
        :func:`_make_composite_meta`
    :return: True if any of the components has been modified, False otherwise.
    :raises Value Error: if proposed new components do not match any existing
        components.
    """
    errors = []
    newer = False
    old_components = old_composite_meta['components']
    new_components = new_composite_meta['components']
    for i, old_component in enumerate(old_components):
        try:
            new_component = new_components[i]
        except IndexError:
            errors.append("Missing builder at index %d" % i)
            continue
        try:
            # check we have same component builder in this position vs existing
            check_same_builder(old_component, new_component)
            newer |= is_builder_newer(old_component, new_component)
        except ValueError as err:
            errors.append("Invalid builder change at index %d: %s" % (i, err))

    for j, new_component in enumerate(new_components[i + 1:], start=i + 1):
        errors.append("Unexpected extra builder at index %d: %r" %
                      (j, new_component))
    if errors:
        raise ValueError('\n'.join(errors))
    return newer


def check_builder_ids(builders):
    """
    Check that all builders in the given list have id's assigned and that no
    id appears more than once in the list.

    :param builders: a list instances of
        :class:`swift.common.ring.builder.RingBuilder`
    :raises: ValueError if any builder id is missing or repeated
    """
    id2index = defaultdict(list)
    errors = []
    for i, builder in enumerate(builders):
        try:
            id2index[builder.id].append(str(i))
        except AttributeError as err:
            errors.append("Problem with builder at index %d: %s" % (i, err))

    for builder_id, index in id2index.items():
        if len(index) > 1:
            errors.append("Builder id %r used at indexes %s" %
                          (builder_id, ', '.join(index)))

    if errors:
        raise ValueError('\n'.join(errors))


class CompositeRingBuilder(object):
    """
    Provides facility to create, persist, load, rebalance  and update composite
    rings, for example::

        # create a CompositeRingBuilder instance with a list of
        # component builder files
        crb = CompositeRingBuilder(["region1.builder", "region2.builder"])

        # perform a cooperative rebalance of the component builders
        crb.rebalance()

        # call compose which will make a new RingData instance
        ring_data = crb.compose()

        # save the composite ring file
        ring_data.save("composite_ring.gz")

        # save the composite metadata file
        crb.save("composite_builder.composite")

        # load the persisted composite metadata file
        crb = CompositeRingBuilder.load("composite_builder.composite")

        # compose (optionally update the paths to the component builder files)
        crb.compose(["/path/to/region1.builder", "/path/to/region2.builder"])

    Composite ring metadata is persisted to file in JSON format. The metadata
    has the structure shown below (using example values)::

      {
        "version": 4,
        "components": [
          {
            "version": 3,
            "id": "8e56f3b692d43d9a666440a3d945a03a",
            "replicas": 1
          },
          {
            "version": 5,
            "id": "96085923c2b644999dbfd74664f4301b",
            "replicas": 1
          }
        ]
        "component_builder_files": {
            "8e56f3b692d43d9a666440a3d945a03a": "/etc/swift/region1.builder",
            "96085923c2b644999dbfd74664f4301b": "/etc/swift/region2.builder",
        }
        "serialization_version": 1,
        "saved_path": "/etc/swift/multi-ring-1.composite",
      }

    `version` is an integer representing the current version of the composite
    ring, which increments each time the ring is successfully (re)composed.

    `components` is a list of dicts, each of which describes relevant
    properties of a component ring

    `component_builder_files` is a dict that maps component ring builder ids to
    the file from which that component ring builder was loaded.

    `serialization_version` is an integer constant.

    `saved_path` is the path to which the metadata was written.

    :params builder_files: a list of paths to builder files that will be used
        as components of the composite ring.
    """
    def __init__(self, builder_files=None):
        self.version = 0
        self.components = []
        self.ring_data = None
        self._builder_files = None
        self._set_builder_files(builder_files or [])
        self._builders = None  # these are lazy loaded in _load_components

    def _set_builder_files(self, builder_files):
        self._builder_files = [os.path.abspath(bf) for bf in builder_files]

    @classmethod
    def load(cls, path_to_file):
        """
        Load composite ring metadata.

        :param path_to_file: Absolute path to a composite ring JSON file.
        :return: an instance of :class:`CompositeRingBuilder`
        :raises IOError: if there is a problem opening the file
        :raises ValueError: if the file does not contain valid composite ring
                            metadata
        """
        try:
            with open(path_to_file, 'rt') as fp:
                metadata = json.load(fp)
            builder_files = [metadata['component_builder_files'][comp['id']]
                             for comp in metadata['components']]

            builder = CompositeRingBuilder(builder_files)
            builder.components = metadata['components']
            builder.version = metadata['version']
        except (ValueError, TypeError, KeyError):
            raise ValueError("File does not contain valid composite ring data")
        return builder

    def to_dict(self):
        """
        Transform the composite ring attributes to a dict. See
        :class:`CompositeRingBuilder` for details of the persisted metadata
        format.

        :return: a composite ring metadata dict
        """
        id2builder_file = dict((component['id'], self._builder_files[i])
                               for i, component in enumerate(self.components))
        return {'components': self.components,
                'component_builder_files': id2builder_file,
                'version': self.version}

    def save(self, path_to_file):
        """
        Save composite ring metadata to given file. See
        :class:`CompositeRingBuilder` for details of the persisted metadata
        format.

        :param path_to_file: Absolute path to a composite ring file
        :raises ValueError: if no composite ring has been built yet with this
                            instance
        """
        if not self.components or not self._builder_files:
            raise ValueError("No composed ring to save.")
        # persist relative paths to builder files
        with open(path_to_file, 'wt') as fp:
            metadata = self.to_dict()
            # future-proofing:
            # - saving abs path to component builder files and this file should
            # allow the relative paths to be derived if required when loading
            # a set of {composite builder file, component builder files} that
            # has been moved, so long as their relative locations are
            # unchanged.
            # - save a serialization format version number
            metadata['saved_path'] = os.path.abspath(path_to_file)
            metadata['serialization_version'] = 1
            json.dump(metadata, fp)

    def _load_components(self, builder_files=None, force=False,
                         require_modified=False):
        if self._builders:
            return self._builder_files, self._builders

        builder_files = builder_files or self._builder_files
        if len(builder_files) < 2:
            raise ValueError('Two or more component builders are required.')

        builders = []
        for builder_file in builder_files:
            # each component builder gets a reference to this composite builder
            # so that it can delegate part movement decisions to the composite
            # builder during rebalance
            builders.append(CooperativeRingBuilder.load(builder_file,
                                                        parent_builder=self))
        check_builder_ids(builders)
        new_metadata = _make_composite_metadata(builders)
        if self.components and self._builder_files and not force:
            modified = check_against_existing(self.to_dict(), new_metadata)
            if require_modified and not modified:
                raise ValueError(
                    "None of the component builders has been modified"
                    " since the existing composite ring was built.")
        self._set_builder_files(builder_files)
        self._builders = builders
        return self._builder_files, self._builders

    def load_components(self, builder_files=None, force=False,
                        require_modified=False):
        """
        Loads component ring builders from builder files. Previously loaded
        component ring builders will discarded and reloaded.

        If a list of component ring builder files is given then that will be
        used to load component ring builders. Otherwise, component ring
        builders will be loaded using the list of builder files that was set
        when the instance was constructed.

        In either case, if metadata for an existing composite ring has been
        loaded then the component ring builders are verified for consistency
        with the existing composition of builders, unless the optional
        ``force`` flag if set True.

        :param builder_files: Optional list of paths to ring builder
            files that will be used to load the component ring builders.
            Typically the list of component builder files will have been set
            when the instance was constructed, for example when using the
            load() class method. However, this parameter may be used if the
            component builder file paths have moved, or, in conjunction with
            the ``force`` parameter, if a new list of component builders is to
            be used.
        :param force: if True then do not verify given builders are
            consistent with any existing composite ring (default is False).
        :param require_modified: if True and ``force`` is False, then
            verify that at least one of the given builders has been modified
            since the composite ring was last built (default is False).
        :return: A tuple of (builder files, loaded builders)
        :raises: ValueError if the component ring builders are not suitable for
            composing with each other, or are inconsistent with any existing
            composite ring, or if require_modified is True and there has been
            no change with respect to the existing ring.
        """
        self._builders = None  # force a reload of builders
        return self._load_components(
            builder_files, force, require_modified)

    def compose(self, builder_files=None, force=False, require_modified=False):
        """
        Builds a composite ring using component ring builders loaded from a
        list of builder files and updates composite ring metadata.

        If a list of component ring builder files is given then that will be
        used to load component ring builders. Otherwise, component ring
        builders will be loaded using the list of builder files that was set
        when the instance was constructed.

        In either case, if metadata for an existing composite ring has been
        loaded then the component ring builders are verified for consistency
        with the existing composition of builders, unless the optional
        ``force`` flag if set True.

        :param builder_files: Optional list of paths to ring builder
            files that will be used to load the component ring builders.
            Typically the list of component builder files will have been set
            when the instance was constructed, for example when using the
            load() class method. However, this parameter may be used if the
            component builder file paths have moved, or, in conjunction with
            the ``force`` parameter, if a new list of component builders is to
            be used.
        :param force: if True then do not verify given builders are
            consistent with any existing composite ring (default is False).
        :param require_modified: if True and ``force`` is False, then
            verify that at least one of the given builders has been modified
            since the composite ring was last built (default is False).
        :return: An instance of :class:`swift.common.ring.ring.RingData`
        :raises: ValueError if the component ring builders are not suitable for
            composing with each other, or are inconsistent with any existing
            composite ring, or if require_modified is True and there has been
            no change with respect to the existing ring.
        """
        self.load_components(builder_files, force=force,
                             require_modified=require_modified)
        self.ring_data = compose_rings(self._builders)
        self.version += 1
        new_metadata = _make_composite_metadata(self._builders)
        self.components = new_metadata['components']
        return self.ring_data

    def rebalance(self):
        """
        Cooperatively rebalances all component ring builders.

        This method does not change the state of the composite ring; a
        subsequent call to :meth:`compose` is required to generate updated
        composite :class:`RingData`.

        :return: A list of dicts, one per component builder, each having the
            following keys:

            * 'builder_file' maps to the component builder file;
            * 'builder' maps to the corresponding instance of
              :class:`swift.common.ring.builder.RingBuilder`;
            * 'result' maps to the results of the rebalance of that component
              i.e. a tuple of: `(number_of_partitions_altered,
              resulting_balance, number_of_removed_devices)`

            The list has the same order as components in the composite ring.
        :raises RingBuilderError: if there is an error while rebalancing any
            component builder.
        """
        self._load_components()
        self.update_last_part_moves()
        component_builders = list(zip(self._builder_files, self._builders))
        # don't let the same builder go first each time
        shuffle(component_builders)
        results = {}
        for builder_file, builder in component_builders:
            try:
                results[builder] = {
                    'builder': builder,
                    'builder_file': builder_file,
                    'result': builder.rebalance()
                }
                builder.validate()
            except RingBuilderError as err:
                self._builders = None
                raise RingBuilderError(
                    'An error occurred while rebalancing component %s: %s' %
                    (builder_file, err))

        for builder_file, builder in component_builders:
            builder.save(builder_file)
        # return results in component order
        return [results[builder] for builder in self._builders]

    def can_part_move(self, part):
        """
        Check with all component builders that it is ok to move a partition.

        :param part: The partition to check.
        :return: True if all component builders agree that the partition can be
            moved, False otherwise.
        """
        # Called by component builders.
        return all(b.can_part_move(part) for b in self._builders)

    def update_last_part_moves(self):
        """
        Updates the record of how many hours ago each partition was moved in
        all component builders.
        """
        # Called at start of each composite rebalance. We need all component
        # builders to be at same last_part_moves epoch before any builder
        # starts moving parts; this will effectively be a no-op for builders
        # that have already been updated in last hour
        for b in self._builders:
            b.update_last_part_moves()


class CooperativeRingBuilder(RingBuilder):
    """
    A subclass of :class:`RingBuilder` that participates in cooperative
    rebalance.

    During rebalance this subclass will consult with its `parent_builder`
    before moving a partition. The `parent_builder` may in turn check with
    co-builders (including this instance) to verify that none have moved that
    partition in the last `min_part_hours`.

    :param part_power: number of partitions = 2**part_power.
    :param replicas: number of replicas for each partition.
    :param min_part_hours: minimum number of hours between partition changes.
    :param parent_builder: an instance of :class:`CompositeRingBuilder`.
    """
    def __init__(self, part_power, replicas, min_part_hours, parent_builder):
        super(CooperativeRingBuilder, self).__init__(
            part_power, replicas, min_part_hours)
        self.parent_builder = parent_builder

    def _can_part_move(self, part):
        # override superclass method to delegate to the parent builder
        return self.parent_builder.can_part_move(part)

    def can_part_move(self, part):
        """
        Check that in the context of this builder alone it is ok to move a
        partition.

        :param part: The partition to check.
        :return: True if the partition can be moved, False otherwise.
        """
        # called by parent_builder - now forward to the superclass
        return (not self.ever_rebalanced or
                super(CooperativeRingBuilder, self)._can_part_move(part))

    def _update_last_part_moves(self):
        # overrides superclass method - parent builder should have called
        # update_last_part_moves() before rebalance; calling the superclass
        # method here would reset _part_moved_bitmap which is state we rely on
        # when min_part_hours is zero
        pass

    def update_last_part_moves(self):
        """
        Updates the record of how many hours ago each partition was moved in
        in this builder.
        """
        # called by parent_builder - now forward to the superclass
        return super(CooperativeRingBuilder, self)._update_last_part_moves()
