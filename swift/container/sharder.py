# Copyright (c) 2015 OpenStack Foundation
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
import collections
import errno
import json
import operator
import time
from collections import defaultdict
from operator import itemgetter
from random import random

import os
import six
from six.moves.urllib.parse import quote
from eventlet import Timeout
from contextlib import contextmanager

from swift.common import internal_client
from swift.common.constraints import check_drive, AUTO_CREATE_ACCOUNT_PREFIX
from swift.common.direct_client import (direct_put_container,
                                        DirectClientException)
from swift.common.request_helpers import USE_REPLICATION_NETWORK_HEADER
from swift.common.ring.utils import is_local_device
from swift.common.swob import str_to_wsgi
from swift.common.utils import get_logger, config_true_value, \
    dump_recon_cache, whataremyips, Timestamp, ShardRange, GreenAsyncPile, \
    config_positive_int_value, quorum_size, parse_override_options, \
    Everything, config_auto_int_value, ShardRangeList, config_percent_value
from swift.container.backend import ContainerBroker, \
    RECORD_TYPE_SHARD, UNSHARDED, SHARDING, SHARDED, COLLAPSED, \
    SHARD_UPDATE_STATES, sift_shard_ranges
from swift.container.replicator import ContainerReplicator


CLEAVE_SUCCESS = 0
CLEAVE_FAILED = 1
CLEAVE_EMPTY = 2


def sharding_enabled(broker):
    # NB all shards will by default have been created with
    # X-Container-Sysmeta-Sharding set and will therefore be candidates for
    # sharding, along with explicitly configured root containers.
    sharding = broker.metadata.get('X-Container-Sysmeta-Sharding')
    if sharding and config_true_value(sharding[0]):
        return True
    # if broker has been marked deleted it will have lost sysmeta, but we still
    # need to process the broker (for example, to shrink any shard ranges) so
    # fallback to checking if it has any shard ranges
    if broker.get_shard_ranges():
        return True
    return False


def make_shard_ranges(broker, shard_data, shards_account_prefix):
    timestamp = Timestamp.now()
    shard_ranges = []
    for data in shard_data:
        # Make a copy so we don't mutate the original
        kwargs = data.copy()
        path = ShardRange.make_path(
            shards_account_prefix + broker.root_account,
            broker.root_container, broker.container,
            timestamp, kwargs.pop('index'))

        shard_ranges.append(ShardRange(path, timestamp, **kwargs))
    return shard_ranges


def _find_discontinuity(paths, start):
    # select the path that reaches furthest from start into the namespace
    start_paths = [path for path in paths if path.lower == start]
    start_paths.sort(key=lambda p: p.upper)
    longest_start_path = start_paths[-1]
    # search for paths that end further into the namespace (note: these must
    # have a lower that differs from the start_path upper, otherwise they would
    # be part of the start_path longer!)
    end_paths = [path for path in paths
                 if path.upper > longest_start_path.upper]
    if end_paths:
        # select those that begin nearest the start of the namespace
        end_paths.sort(key=lambda p: p.lower)
        end_paths = [p for p in end_paths if p.lower == end_paths[0].lower]
        # select the longest of those
        end_paths.sort(key=lambda p: p.upper)
        longest_end_path = end_paths[-1]
    else:
        longest_end_path = None
    return longest_start_path, longest_end_path


def find_paths_with_gaps(shard_ranges, within_range=None):
    """
    Find gaps in the shard ranges and pairs of shard range paths that lead to
    and from those gaps. For each gap a single pair of adjacent paths is
    selected. The concatenation of all selected paths and gaps will span the
    entire namespace with no overlaps.

    :param shard_ranges: a list of instances of ShardRange.
    :param within_range: an optional ShardRange that constrains the search
        space; the method will only return gaps within this range. The default
        is the entire namespace.
    :return: A list of tuples of ``(start_path, gap_range, end_path)`` where
        ``start_path`` is a list of ShardRanges leading to the gap,
        ``gap_range`` is a ShardRange synthesized to describe the namespace
        gap, and ``end_path`` is a list of ShardRanges leading from the gap.
        When gaps start or end at the namespace minimum or maximum bounds,
        ``start_path`` and ``end_path`` may be 'null' paths that contain a
        single ShardRange covering either the minimum or maximum of the
        namespace.
    """
    timestamp = Timestamp.now()
    within_range = within_range or ShardRange('entire/namespace', timestamp)
    shard_ranges = ShardRangeList(shard_ranges)
    # note: find_paths results do not include shrinking ranges
    paths = find_paths(shard_ranges)
    # add paths covering no namespace at start and end of namespace to ensure
    # that a start_path and end_path is always found even when there is a gap
    # at the start or end of the namespace
    null_start = ShardRange('null/start', timestamp,
                            lower=ShardRange.MIN,
                            upper=ShardRange.MIN,
                            state=ShardRange.FOUND)
    null_end = ShardRange('null/end', timestamp,
                          lower=ShardRange.MAX,
                          upper=ShardRange.MAX,
                          state=ShardRange.FOUND)
    paths.extend([ShardRangeList([null_start]), ShardRangeList([null_end])])
    paths_with_gaps = []
    start = null_start.lower
    while True:
        start_path, end_path = _find_discontinuity(paths, start)
        if end_path is None:
            # end of namespace reached
            break
        start = end_path.lower
        if start_path.upper > end_path.lower:
            # overlap
            continue
        gap_range = ShardRange('gap/index_%06d' % len(paths_with_gaps),
                               timestamp,
                               lower=start_path.upper,
                               upper=end_path.lower)
        if gap_range.overlaps(within_range):
            paths_with_gaps.append((start_path, gap_range, end_path))
    return paths_with_gaps


def _is_parent_or_child(shard_range, other, time_period):
    """
    Test if shard range ``shard_range`` is the parent or a child of another
    shard range ``other`` within past time period ``time_period``. This method
    is limited to work only within the scope of the same user-facing account
    (with and without shard prefix).

    :param shard_range: an instance of ``ShardRange``.
    :param other: an instance of ``ShardRange``.
    :param time_period: the specified past time period in seconds. Value of
        0 means all time in the past.
    :return: True if ``shard_range`` is the parent or a child of ``other``
        within past time period, False otherwise, assuming that they are within
         the same account.
    """
    exclude_age = (time.time() - float(time_period)) if time_period > 0 else 0
    if shard_range.is_child_of(other) and shard_range.timestamp >= exclude_age:
        return True
    if other.is_child_of(shard_range) and other.timestamp >= exclude_age:
        return True
    return False


def find_overlapping_ranges(
        shard_ranges, exclude_parent_child=False, time_period=0):
    """
    Find all pairs of overlapping ranges in the given list.

    :param shard_ranges: A list of :class:`~swift.utils.ShardRange`
    :param exclude_parent_child: If True then overlapping pairs that have a
        parent-child relationship within the past time period
        ``time_period`` are excluded from the returned set. Default is
        False.
    :param time_period: the specified past time period in seconds. Value of
        0 means all time in the past.
    :return: a set of tuples, each tuple containing ranges that overlap with
        each other.
    """
    result = set()
    for i, shard_range in enumerate(shard_ranges):
        if exclude_parent_child:
            overlapping = [
                sr for sr in shard_ranges[i + 1:]
                if shard_range.name != sr.name and shard_range.overlaps(sr) and
                not _is_parent_or_child(shard_range, sr, time_period)]
        else:
            overlapping = [
                sr for sr in shard_ranges[i + 1:]
                if shard_range.name != sr.name and shard_range.overlaps(sr)]
        if overlapping:
            overlapping.append(shard_range)
            overlapping.sort(key=ShardRange.sort_key)
            result.add(tuple(overlapping))

    return result


def is_sharding_candidate(shard_range, threshold):
    # note: use *object* count as the condition for sharding: tombstones will
    # eventually be reclaimed so should not trigger sharding
    return (shard_range.state == ShardRange.ACTIVE and
            shard_range.object_count >= threshold)


def is_shrinking_candidate(shard_range, shrink_threshold, expansion_limit,
                           states=None):
    # typically shrink_threshold < expansion_limit but check both just in case
    # note: use *row* count (objects plus tombstones) as the condition for
    # shrinking to avoid inadvertently moving large numbers of tombstones into
    # an acceptor
    states = states or (ShardRange.ACTIVE,)
    return (shard_range.state in states and
            shard_range.row_count < shrink_threshold and
            shard_range.row_count <= expansion_limit)


def find_sharding_candidates(broker, threshold, shard_ranges=None):
    # this should only execute on root containers; the goal is to find
    # large shard containers that should be sharded.
    # First cut is simple: assume root container shard usage stats are good
    # enough to make decision.
    if shard_ranges is None:
        shard_ranges = broker.get_shard_ranges(states=[ShardRange.ACTIVE])
    candidates = []
    for shard_range in shard_ranges:
        if not is_sharding_candidate(shard_range, threshold):
            continue
        shard_range.update_state(ShardRange.SHARDING,
                                 state_timestamp=Timestamp.now())
        shard_range.epoch = shard_range.state_timestamp
        candidates.append(shard_range)
    return candidates


def find_shrinking_candidates(broker, shrink_threshold, expansion_limit):
    # this is only here to preserve a legacy public function signature;
    # superseded by find_compactible_shard_sequences
    merge_pairs = {}
    # restrict search to sequences with one donor
    results = find_compactible_shard_sequences(broker, shrink_threshold,
                                               expansion_limit, 1, -1,
                                               include_shrinking=True)
    for sequence in results:
        # map acceptor -> donor list
        merge_pairs[sequence[-1]] = sequence[-2]
    return merge_pairs


def find_compactible_shard_sequences(broker,
                                     shrink_threshold,
                                     expansion_limit,
                                     max_shrinking,
                                     max_expanding,
                                     include_shrinking=False):
    """
    Find sequences of shard ranges that could be compacted into a single
    acceptor shard range.

    This function does not modify shard ranges.

    :param broker: A :class:`~swift.container.backend.ContainerBroker`.
    :param shrink_threshold: the number of rows below which a shard may be
        considered for shrinking into another shard
    :param expansion_limit: the maximum number of rows that an acceptor shard
        range should have after other shard ranges have been compacted into it
    :param max_shrinking: the maximum number of shard ranges that should be
        compacted into each acceptor; -1 implies unlimited.
    :param max_expanding: the maximum number of acceptors to be found (i.e. the
        maximum number of sequences to be returned); -1 implies unlimited.
    :param include_shrinking: if True then existing compactible sequences are
        included in the results; default is False.
    :returns: A list of :class:`~swift.common.utils.ShardRangeList` each
        containing a sequence of neighbouring shard ranges that may be
        compacted; the final shard range in the list is the acceptor
    """
    # this should only execute on root containers that have sharded; the
    # goal is to find small shard containers that could be retired by
    # merging with a neighbour.
    # First cut is simple: assume root container shard usage stats are good
    # enough to make decision; only merge with upper neighbour so that
    # upper bounds never change (shard names include upper bound).
    shard_ranges = broker.get_shard_ranges()
    own_shard_range = broker.get_own_shard_range()

    def sequence_complete(sequence):
        # a sequence is considered complete if any of the following are true:
        #  - the final shard range has more objects than the shrink_threshold,
        #    so should not be shrunk (this shard will be the acceptor)
        #  - the max number of shard ranges to be compacted (max_shrinking) has
        #    been reached
        #  - the total number of objects in the sequence has reached the
        #    expansion_limit
        if (sequence and
                (not is_shrinking_candidate(
                    sequence[-1], shrink_threshold, expansion_limit,
                    states=(ShardRange.ACTIVE, ShardRange.SHRINKING)) or
                 0 < max_shrinking < len(sequence) or
                 sequence.row_count >= expansion_limit)):
            return True
        return False

    compactible_sequences = []
    index = 0
    expanding = 0
    while ((max_expanding < 0 or expanding < max_expanding) and
           index < len(shard_ranges)):
        if not is_shrinking_candidate(
                shard_ranges[index], shrink_threshold, expansion_limit,
                states=(ShardRange.ACTIVE, ShardRange.SHRINKING)):
            # this shard range cannot be the start of a new or existing
            # compactible sequence, move on
            index += 1
            continue

        # start of a *possible* sequence
        sequence = ShardRangeList([shard_ranges[index]])
        for shard_range in shard_ranges[index + 1:]:
            # attempt to add contiguous shard ranges to the sequence
            if sequence.upper < shard_range.lower:
                # found a gap! break before consuming this range because it
                # could become the first in the next sequence
                break

            if shard_range.state not in (ShardRange.ACTIVE,
                                         ShardRange.SHRINKING):
                # found? created? sharded? don't touch it
                break

            if shard_range.state == ShardRange.SHRINKING:
                # already shrinking: add to sequence unconditionally
                sequence.append(shard_range)
            elif (sequence.row_count + shard_range.row_count
                  <= expansion_limit):
                # add to sequence: could be a donor or acceptor
                sequence.append(shard_range)
                if sequence_complete(sequence):
                    break
            else:
                break

        index += len(sequence)
        if (index == len(shard_ranges) and
                len(shard_ranges) == len(sequence) and
                not sequence_complete(sequence) and
                sequence.includes(own_shard_range)):
            # special case: only one sequence has been found, which consumes
            # all shard ranges, encompasses the entire namespace, has no more
            # than expansion_limit records and whose shard ranges are all
            # shrinkable; all the shards in the sequence can be shrunk to the
            # root, so append own_shard_range to the sequence to act as an
            # acceptor; note: only shrink to the root when *all* the remaining
            # shard ranges can be simultaneously shrunk to the root.
            sequence.append(own_shard_range)

        if len(sequence) < 2 or sequence[-1].state not in (ShardRange.ACTIVE,
                                                           ShardRange.SHARDED):
            # this sequence doesn't end with a suitable acceptor shard range
            continue

        # all valid sequences are counted against the max_expanding allowance
        # even if the sequence is already shrinking
        expanding += 1
        if (all([sr.state != ShardRange.SHRINKING for sr in sequence]) or
                include_shrinking):
            compactible_sequences.append(sequence)

    return compactible_sequences


def finalize_shrinking(broker, acceptor_ranges, donor_ranges, timestamp):
    """
    Update donor shard ranges to shrinking state and merge donors and acceptors
    to broker.

    :param broker: A :class:`~swift.container.backend.ContainerBroker`.
    :param acceptor_ranges: A list of :class:`~swift.common.utils.ShardRange`
        that are to be acceptors.
    :param donor_ranges: A list of :class:`~swift.common.utils.ShardRange`
        that are to be donors; these will have their state and timestamp
        updated.
    :param timestamp: timestamp to use when updating donor state
    """
    for donor in donor_ranges:
        if donor.update_state(ShardRange.SHRINKING):
            # Set donor state to shrinking state_timestamp defines new epoch
            donor.epoch = donor.state_timestamp = timestamp
    broker.merge_shard_ranges(acceptor_ranges + donor_ranges)


def process_compactible_shard_sequences(broker, sequences):
    """
    Transform the given sequences of shard ranges into a list of acceptors and
    a list of shrinking donors. For each given sequence the final ShardRange in
    the sequence (the acceptor) is expanded to accommodate the other
    ShardRanges in the sequence (the donors). The donors and acceptors are then
    merged into the broker.

    :param broker: A :class:`~swift.container.backend.ContainerBroker`.
    :param sequences: A list of :class:`~swift.common.utils.ShardRangeList`
    """
    timestamp = Timestamp.now()
    acceptor_ranges = []
    shrinking_ranges = []
    for sequence in sequences:
        donors = sequence[:-1]
        shrinking_ranges.extend(donors)
        # Update the acceptor container with its expanded bounds to prevent it
        # treating objects cleaved from the donor as misplaced.
        acceptor = sequence[-1]
        if acceptor.expand(donors):
            # Update the acceptor container with its expanded bounds to prevent
            # it treating objects cleaved from the donor as misplaced.
            acceptor.timestamp = timestamp
        if acceptor.update_state(ShardRange.ACTIVE):
            # Ensure acceptor state is ACTIVE (when acceptor is root)
            acceptor.state_timestamp = timestamp
        acceptor_ranges.append(acceptor)
    finalize_shrinking(broker, acceptor_ranges, shrinking_ranges, timestamp)


def find_paths(shard_ranges):
    """
    Returns a list of all continuous paths through the shard ranges. An
    individual path may not necessarily span the entire namespace, but it will
    span a continuous namespace without gaps.

    :param shard_ranges: A list of :class:`~swift.common.utils.ShardRange`.
    :return: A list of :class:`~swift.common.utils.ShardRangeList`.
    """
    # A node is a point in the namespace that is used as a bound of any shard
    # range. Shard ranges form the edges between nodes.

    # First build a dict mapping nodes to a list of edges that leave that node
    # (in other words, shard ranges whose lower bound equals the node)
    node_successors = collections.defaultdict(list)
    for shard_range in shard_ranges:
        if shard_range.state == ShardRange.SHRINKING:
            # shrinking shards are not a viable edge in any path
            continue
        node_successors[shard_range.lower].append(shard_range)

    paths = []

    def clone_path(other=None):
        # create a new path, possibly cloning another path, and add it to the
        # list of all paths through the shards
        path = ShardRangeList() if other is None else ShardRangeList(other)
        paths.append(path)
        return path

    # we need to keep track of every path that ends at each node so that when
    # we visit the node we can extend those paths, or clones of them, with the
    # edges that leave the node
    paths_to_node = collections.defaultdict(list)

    # visit the nodes in ascending order by name...
    for node, edges in sorted(node_successors.items()):
        if not edges:
            # this node is a dead-end, so there's no path updates to make
            continue
        if not paths_to_node[node]:
            # this is either the first node to be visited, or it has no paths
            # leading to it, so we need to start a new path here
            paths_to_node[node].append(clone_path([]))
        for path_to_node in paths_to_node[node]:
            # extend each path that arrives at this node with all of the
            # possible edges that leave the node; if more than edge leaves the
            # node then we will make clones of the path to the node and extend
            # those clones, adding to the collection of all paths though the
            # shards
            for i, edge in enumerate(edges):
                if i == len(edges) - 1:
                    # the last edge is used to extend the original path to the
                    # node; there is nothing special about the last edge, but
                    # doing this last means the original path to the node can
                    # be cloned for all other edges before being modified here
                    path = path_to_node
                else:
                    # for all but one of the edges leaving the node we need to
                    # make a clone the original path
                    path = clone_path(path_to_node)
                # extend the path with the edge
                path.append(edge)
                # keep track of which node this path now arrives at
                paths_to_node[edge.upper].append(path)
    return paths


def rank_paths(paths, shard_range_to_span):
    """
    Sorts the given list of paths such that the most preferred path is the
    first item in the list.

    :param paths: A list of :class:`~swift.common.utils.ShardRangeList`.
    :param shard_range_to_span: An instance of
        :class:`~swift.common.utils.ShardRange` that describes the namespace
        that would ideally be spanned by a path. Paths that include this
        namespace will be preferred over those that do not.
    :return: A sorted list of :class:`~swift.common.utils.ShardRangeList`.
    """
    def sort_key(path):
        # defines the order of preference for paths through shards
        return (
            # complete path for the namespace
            path.includes(shard_range_to_span),
            # most cleaving progress
            path.find_lower(lambda sr: sr.state not in (
                ShardRange.CLEAVED, ShardRange.ACTIVE)),
            # largest object count
            path.object_count,
            # fewest timestamps
            -1 * len(path.timestamps),
            # newest timestamp
            sorted(path.timestamps)[-1]
        )

    paths.sort(key=sort_key, reverse=True)
    return paths


def combine_shard_ranges(new_shard_ranges, existing_shard_ranges):
    """
    Combines new and existing shard ranges based on most recent state.

    :param new_shard_ranges: a list of ShardRange instances.
    :param existing_shard_ranges: a list of ShardRange instances.
    :return: a list of ShardRange instances.
    """
    new_shard_ranges = [dict(sr) for sr in new_shard_ranges]
    existing_shard_ranges = [dict(sr) for sr in existing_shard_ranges]
    to_add, to_delete = sift_shard_ranges(
        new_shard_ranges,
        dict((sr['name'], sr) for sr in existing_shard_ranges))
    result = [ShardRange.from_dict(existing)
              for existing in existing_shard_ranges
              if existing['name'] not in to_delete]
    result.extend([ShardRange.from_dict(sr) for sr in to_add])
    return sorted([sr for sr in result if not sr.deleted],
                  key=ShardRange.sort_key)


def update_own_shard_range_stats(broker, own_shard_range):
    """
    Update the ``own_shard_range`` with the up-to-date object stats from
    the ``broker``.

    Note: this method does not persist the updated ``own_shard_range``;
    callers should use ``broker.merge_shard_ranges`` if the updated stats
    need to be persisted.

    :param broker: an instance of ``ContainerBroker``.
    :param own_shard_range: and instance of ``ShardRange``.
    :returns: ``own_shard_range`` with up-to-date ``object_count``
        and ``bytes_used``.
    """
    info = broker.get_info()
    own_shard_range.update_meta(
        info['object_count'], info['bytes_used'])
    return own_shard_range


class CleavingContext(object):
    """
    Encapsulates metadata associated with the process of cleaving a retiring
    DB. This metadata includes:

    * ``ref``: The unique part of the key that is used when persisting a
      serialized ``CleavingContext`` as sysmeta in the DB. The unique part of
      the key is based off the DB id. This ensures that each context is
      associated with a specific DB file. The unique part of the key is
      included in the ``CleavingContext`` but should not be modified by any
      caller.

    * ``cursor``: the upper bound of the last shard range to have been
      cleaved from the retiring DB.

    * ``max_row``: the retiring DB's max row; this is updated to the value of
      the retiring DB's ``max_row`` every time a ``CleavingContext`` is
      loaded for that DB, and may change during the process of cleaving the
      DB.

    * ``cleave_to_row``: the value of ``max_row`` at the moment when cleaving
      starts for the DB. When cleaving completes (i.e. the cleave cursor has
      reached the upper bound of the cleaving namespace), ``cleave_to_row``
      is compared to the current ``max_row``: if the two values are not equal
      then rows have been added to the DB which may not have been cleaved, in
      which case the ``CleavingContext`` is ``reset`` and cleaving is
      re-started.

    * ``last_cleave_to_row``: the minimum DB row from which cleaving should
      select objects to cleave; this is initially set to None i.e. all rows
      should be cleaved. If the ``CleavingContext`` is ``reset`` then the
      ``last_cleave_to_row`` is set to the current value of
      ``cleave_to_row``, which in turn is set to the current value of
      ``max_row`` by a subsequent call to ``start``. The repeated cleaving
      therefore only selects objects in rows greater than the
      ``last_cleave_to_row``, rather than cleaving the whole DB again.

    * ``ranges_done``: the number of shard ranges that have been cleaved from
      the retiring DB.

    * ``ranges_todo``: the number of shard ranges that are yet to be
      cleaved from the retiring DB.
    """
    def __init__(self, ref, cursor='', max_row=None, cleave_to_row=None,
                 last_cleave_to_row=None, cleaving_done=False,
                 misplaced_done=False, ranges_done=0, ranges_todo=0):
        self.ref = ref
        self._cursor = None
        self.cursor = cursor
        self.max_row = max_row
        self.cleave_to_row = cleave_to_row
        self.last_cleave_to_row = last_cleave_to_row
        self.cleaving_done = cleaving_done
        self.misplaced_done = misplaced_done
        self.ranges_done = ranges_done
        self.ranges_todo = ranges_todo

    def __iter__(self):
        yield 'ref', self.ref
        yield 'cursor', self.cursor
        yield 'max_row', self.max_row
        yield 'cleave_to_row', self.cleave_to_row
        yield 'last_cleave_to_row', self.last_cleave_to_row
        yield 'cleaving_done', self.cleaving_done
        yield 'misplaced_done', self.misplaced_done
        yield 'ranges_done', self.ranges_done
        yield 'ranges_todo', self.ranges_todo

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, ', '.join(
            '%s=%r' % prop for prop in self))

    def _encode(cls, value):
        if value is not None and six.PY2 and isinstance(value, six.text_type):
            return value.encode('utf-8')
        return value

    @property
    def cursor(self):
        return self._cursor

    @cursor.setter
    def cursor(self, value):
        self._cursor = self._encode(value)

    @property
    def marker(self):
        return self.cursor + '\x00'

    @classmethod
    def _make_ref(cls, broker):
        return broker.get_info()['id']

    @classmethod
    def load_all(cls, broker):
        """
        Returns all cleaving contexts stored in the broker's DB.

        :param broker: an instance of :class:`ContainerBroker`
        :return: list of tuples of (CleavingContext, timestamp)
        """
        brokers = broker.get_brokers()
        sysmeta = brokers[-1].get_sharding_sysmeta_with_timestamps()

        contexts = []
        for key, (val, timestamp) in sysmeta.items():
            # If the value is blank, then the metadata is
            # marked for deletion
            if key.startswith("Context-") and val:
                try:
                    contexts.append((cls(**json.loads(val)), timestamp))
                except ValueError:
                    continue
        return contexts

    @classmethod
    def load(cls, broker):
        """
        Returns a CleavingContext tracking the cleaving progress of the given
        broker's DB.

        :param broker: an instances of :class:`ContainerBroker`
        :return: An instance of :class:`CleavingContext`.
        """
        brokers = broker.get_brokers()
        ref = cls._make_ref(brokers[0])
        data = brokers[-1].get_sharding_sysmeta('Context-' + ref)
        data = json.loads(data) if data else {}
        data['ref'] = ref
        data['max_row'] = brokers[0].get_max_row()
        return cls(**data)

    def store(self, broker):
        """
        Persists the serialized ``CleavingContext`` as sysmeta in the given
        broker's DB.

        :param broker: an instances of :class:`ContainerBroker`
        """
        broker.set_sharding_sysmeta('Context-' + self.ref,
                                    json.dumps(dict(self)))

    def reset(self):
        self.cursor = ''
        self.ranges_done = 0
        self.ranges_todo = 0
        self.cleaving_done = False
        self.misplaced_done = False
        self.last_cleave_to_row = self.cleave_to_row

    def start(self):
        self.cursor = ''
        self.ranges_done = 0
        self.ranges_todo = 0
        self.cleaving_done = False
        self.cleave_to_row = self.max_row

    def range_done(self, new_cursor):
        self.ranges_done += 1
        self.ranges_todo -= 1
        self.cursor = new_cursor

    def done(self):
        return all((self.misplaced_done, self.cleaving_done,
                    self.max_row == self.cleave_to_row))

    def delete(self, broker):
        # These will get reclaimed when `_reclaim_metadata` in
        # common/db.py is called.
        broker.set_sharding_sysmeta('Context-' + self.ref, '')


class ContainerSharderConf(object):
    def __init__(self, conf=None):
        conf = conf if conf else {}

        def get_val(key, validator, default):
            """
            Get a value from conf and validate it.

            :param key: key to lookup value in the ``conf`` dict.
            :param validator: A function that will passed the value from the
                ``conf`` dict and should return the value to be set. This
                function should raise a ValueError if the ``conf`` value if not
                valid.
            :param default: value to use if ``key`` is not found in ``conf``.
            :raises: ValueError if the value read from ``conf`` is invalid.
            :returns: the configuration value.
            """
            try:
                return validator(conf.get(key, default))
            except ValueError as err:
                raise ValueError('Error setting %s: %s' % (key, err))

        self.shard_container_threshold = get_val(
            'shard_container_threshold', config_positive_int_value, 1000000)
        self.max_shrinking = get_val(
            'max_shrinking', int, 1)
        self.max_expanding = get_val(
            'max_expanding', int, -1)
        self.shard_scanner_batch_size = get_val(
            'shard_scanner_batch_size', config_positive_int_value, 10)
        self.cleave_batch_size = get_val(
            'cleave_batch_size', config_positive_int_value, 2)
        self.cleave_row_batch_size = get_val(
            'cleave_row_batch_size', config_positive_int_value, 10000)
        self.broker_timeout = get_val(
            'broker_timeout', config_positive_int_value, 60)
        self.recon_candidates_limit = get_val(
            'recon_candidates_limit', int, 5)
        self.recon_sharded_timeout = get_val(
            'recon_sharded_timeout', int, 43200)
        self.container_sharding_timeout = get_val(
            'container_sharding_timeout', int, 172800)
        self.conn_timeout = get_val(
            'conn_timeout', float, 5)
        self.auto_shard = get_val(
            'auto_shard', config_true_value, False)
        # deprecated percent options still loaded...
        self.shrink_threshold = get_val(
            'shard_shrink_point', self.percent_of_threshold, 10)
        self.expansion_limit = get_val(
            'shard_shrink_merge_point', self.percent_of_threshold, 75)
        # ...but superseded by absolute options if present in conf
        self.shrink_threshold = get_val(
            'shrink_threshold', int, self.shrink_threshold)
        self.expansion_limit = get_val(
            'expansion_limit', int, self.expansion_limit)
        self.rows_per_shard = get_val(
            'rows_per_shard', config_positive_int_value,
            max(self.shard_container_threshold // 2, 1))
        self.minimum_shard_size = get_val(
            'minimum_shard_size', config_positive_int_value,
            max(self.rows_per_shard // 5, 1))

    def percent_of_threshold(self, val):
        return int(config_percent_value(val) * self.shard_container_threshold)

    @classmethod
    def validate_conf(cls, namespace):
        ops = {'<': operator.lt,
               '<=': operator.le}
        checks = (('minimum_shard_size', '<=', 'rows_per_shard'),
                  ('shrink_threshold', '<=', 'minimum_shard_size'),
                  ('rows_per_shard', '<', 'shard_container_threshold'),
                  ('expansion_limit', '<', 'shard_container_threshold'))
        for key1, op, key2 in checks:
            try:
                val1 = getattr(namespace, key1)
                val2 = getattr(namespace, key2)
            except AttributeError:
                # swift-manage-shard-ranges uses a subset of conf options for
                # each command so only validate those actually in the namespace
                continue
            if not ops[op](val1, val2):
                raise ValueError('%s (%d) must be %s %s (%d)'
                                 % (key1, val1, op, key2, val2))


DEFAULT_SHARDER_CONF = vars(ContainerSharderConf())


class ContainerSharder(ContainerSharderConf, ContainerReplicator):
    """Shards containers."""
    log_route = 'container-sharder'

    def __init__(self, conf, logger=None):
        logger = logger or get_logger(conf, log_route=self.log_route)
        ContainerReplicator.__init__(self, conf, logger=logger)
        ContainerSharderConf.__init__(self, conf)
        ContainerSharderConf.validate_conf(self)
        if conf.get('auto_create_account_prefix'):
            self.logger.warning('Option auto_create_account_prefix is '
                                'deprecated. Configure '
                                'auto_create_account_prefix under the '
                                'swift-constraints section of '
                                'swift.conf. This option will '
                                'be ignored in a future release.')
            auto_create_account_prefix = \
                self.conf['auto_create_account_prefix']
        else:
            auto_create_account_prefix = AUTO_CREATE_ACCOUNT_PREFIX
        self.shards_account_prefix = (auto_create_account_prefix + 'shards_')
        self.sharding_candidates = []
        self.shrinking_candidates = []
        replica_count = self.ring.replica_count
        quorum = quorum_size(replica_count)
        self.shard_replication_quorum = config_auto_int_value(
            conf.get('shard_replication_quorum'), quorum)
        if self.shard_replication_quorum > replica_count:
            self.logger.warning(
                'shard_replication_quorum of %s exceeds replica count %s'
                ', reducing to %s', self.shard_replication_quorum,
                replica_count, replica_count)
            self.shard_replication_quorum = replica_count
        self.existing_shard_replication_quorum = config_auto_int_value(
            conf.get('existing_shard_replication_quorum'),
            self.shard_replication_quorum)
        if self.existing_shard_replication_quorum > replica_count:
            self.logger.warning(
                'existing_shard_replication_quorum of %s exceeds replica count'
                ' %s, reducing to %s', self.existing_shard_replication_quorum,
                replica_count, replica_count)
            self.existing_shard_replication_quorum = replica_count

        # internal client
        request_tries = config_positive_int_value(
            conf.get('request_tries', 3))
        internal_client_conf_path = conf.get('internal_client_conf_path',
                                             '/etc/swift/internal-client.conf')
        try:
            self.int_client = internal_client.InternalClient(
                internal_client_conf_path,
                'Swift Container Sharder',
                request_tries,
                allow_modify_pipeline=False,
                use_replication_network=True,
                global_conf={'log_name': '%s-ic' % conf.get(
                    'log_name', self.log_route)})
        except (OSError, IOError) as err:
            if err.errno != errno.ENOENT and \
                    not str(err).endswith(' not found'):
                raise
            raise SystemExit(
                'Unable to load internal client from config: %r (%s)' %
                (internal_client_conf_path, err))
        self.stats_interval = float(conf.get('stats_interval', '3600'))
        self.reported = 0

    def _zero_stats(self):
        """Zero out the stats."""
        super(ContainerSharder, self)._zero_stats()
        # all sharding stats that are additional to the inherited replicator
        # stats are maintained under the 'sharding' key in self.stats
        self.stats['sharding'] = defaultdict(lambda: defaultdict(int))
        self.sharding_candidates = []
        self.shrinking_candidates = []

    def _append_stat(self, category, key, value):
        if not self.stats['sharding'][category][key]:
            self.stats['sharding'][category][key] = list()
        self.stats['sharding'][category][key].append(value)

    def _min_stat(self, category, key, value):
        current = self.stats['sharding'][category][key]
        if not current:
            self.stats['sharding'][category][key] = value
        else:
            self.stats['sharding'][category][key] = min(current, value)

    def _max_stat(self, category, key, value):
        current = self.stats['sharding'][category][key]
        if not current:
            self.stats['sharding'][category][key] = value
        else:
            self.stats['sharding'][category][key] = max(current, value)

    def _increment_stat(self, category, key, statsd=False):
        self._update_stat(category, key, step=1, statsd=statsd)

    def _update_stat(self, category, key, step=1, statsd=False):
        if step:
            self.stats['sharding'][category][key] += step
            if statsd:
                statsd_key = '%s_%s' % (category, key)
                self.logger.update_stats(statsd_key, step)

    def _make_stats_info(self, broker, node, own_shard_range):
        try:
            file_size = os.stat(broker.db_file).st_size
        except OSError:
            file_size = None

        return {'path': broker.db_file,
                'node_index': node.get('index'),
                'account': broker.account,
                'container': broker.container,
                'root': broker.root_path,
                'object_count': own_shard_range.object_count,
                'meta_timestamp': own_shard_range.meta_timestamp.internal,
                'file_size': file_size}

    def _identify_sharding_candidate(self, broker, node):
        own_shard_range = broker.get_own_shard_range()
        update_own_shard_range_stats(broker, own_shard_range)
        if is_sharding_candidate(
                own_shard_range, self.shard_container_threshold):
            self.sharding_candidates.append(
                self._make_stats_info(broker, node, own_shard_range))

    def _identify_shrinking_candidate(self, broker, node):
        sequences = find_compactible_shard_sequences(
            broker, self.shrink_threshold, self.expansion_limit,
            self.max_shrinking, self.max_expanding)
        # compactible_ranges are all apart from final acceptor in each sequence
        compactible_ranges = sum(len(seq) - 1 for seq in sequences)

        if compactible_ranges:
            own_shard_range = broker.get_own_shard_range()
            update_own_shard_range_stats(broker, own_shard_range)
            shrink_candidate = self._make_stats_info(
                broker, node, own_shard_range)
            # The number of ranges/donors that can be shrunk if the
            # tool is used with the current max_shrinking, max_expanding
            # settings.
            shrink_candidate['compactible_ranges'] = compactible_ranges
            self.shrinking_candidates.append(shrink_candidate)

    def _transform_candidate_stats(self, category, candidates, sort_keys):
        category['found'] = len(candidates)
        candidates.sort(key=itemgetter(*sort_keys), reverse=True)
        if self.recon_candidates_limit >= 0:
            category['top'] = candidates[:self.recon_candidates_limit]
        else:
            category['top'] = candidates

    def _record_sharding_progress(self, broker, node, error):
        db_state = broker.get_db_state()
        if db_state not in (UNSHARDED, SHARDING, SHARDED):
            return
        own_shard_range = broker.get_own_shard_range()
        if own_shard_range.state not in ShardRange.CLEAVING_STATES:
            return

        if db_state == SHARDED:
            contexts = CleavingContext.load_all(broker)
            if not contexts:
                return
            context_ts = max(float(ts) for c, ts in contexts)
            if context_ts + self.recon_sharded_timeout \
                    < float(Timestamp.now()):
                # last context timestamp too old for the
                # broker to be recorded
                return

        update_own_shard_range_stats(broker, own_shard_range)
        info = self._make_stats_info(broker, node, own_shard_range)
        info['state'] = own_shard_range.state_text
        info['db_state'] = broker.get_db_state()
        states = [ShardRange.FOUND, ShardRange.CREATED,
                  ShardRange.CLEAVED, ShardRange.ACTIVE]
        shard_ranges = broker.get_shard_ranges(states=states)
        state_count = {}
        for state in states:
            state_count[ShardRange.STATES[state]] = 0
        for shard_range in shard_ranges:
            state_count[shard_range.state_text] += 1
        info.update(state_count)
        info['error'] = error and str(error)
        self._append_stat('sharding_in_progress', 'all', info)

        if broker.sharding_required() and (
                own_shard_range.epoch is not None) and (
                float(own_shard_range.epoch) +
                self.container_sharding_timeout <
                time.time()):
            # Note: There is no requirement that own_shard_range.epoch equals
            # the time at which the own_shard_range was merged into the
            # container DB, which predicates sharding starting. But s-m-s-r and
            # auto-sharding do set epoch and then merge, so we use it to tell
            # whether sharding has been taking too long or not.
            self.logger.warning(
                'Cleaving has not completed in %.2f seconds since %s.'
                ' Container DB file and path: %s (%s), DB state: %s,'
                ' own_shard_range state: %s, state count of shard ranges: %s' %
                (time.time() - float(own_shard_range.epoch),
                 own_shard_range.epoch.isoformat, broker.db_file,
                 quote(broker.path), db_state,
                 own_shard_range.state_text, str(state_count)))

    def _report_stats(self):
        # report accumulated stats since start of one sharder cycle
        default_stats = ('attempted', 'success', 'failure')
        category_keys = (
            ('visited', default_stats + ('skipped', 'completed')),
            ('scanned', default_stats + ('found', 'min_time', 'max_time')),
            ('created', default_stats),
            ('cleaved', default_stats + ('min_time', 'max_time',)),
            ('misplaced', default_stats + ('found', 'placed', 'unplaced')),
            ('audit_root', default_stats + ('has_overlap', 'num_overlap')),
            ('audit_shard', default_stats),
        )

        now = time.time()
        last_report = time.ctime(self.stats['start'])
        elapsed = now - self.stats['start']
        sharding_stats = self.stats['sharding']
        for category, keys in category_keys:
            stats = sharding_stats[category]
            msg = ' '.join(['%s:%s' % (k, str(stats[k])) for k in keys])
            self.logger.info('Since %s %s - %s', last_report, category, msg)

        # transform the sharding and shrinking candidate states
        # first sharding
        category = self.stats['sharding']['sharding_candidates']
        self._transform_candidate_stats(category, self.sharding_candidates,
                                        sort_keys=('object_count',))

        # next shrinking
        category = self.stats['sharding']['shrinking_candidates']
        self._transform_candidate_stats(category, self.shrinking_candidates,
                                        sort_keys=('compactible_ranges',))

        dump_recon_cache(
            {'sharding_stats': self.stats,
             'sharding_time': elapsed,
             'sharding_last': now},
            self.rcache, self.logger)
        self.reported = now

    def _periodic_report_stats(self):
        if (time.time() - self.reported) >= self.stats_interval:
            self._report_stats()

    def _check_node(self, node):
        """
        :return: The path to the device, if the node is mounted.
            Returns False if the node is unmounted.
        """
        if not node:
            return False
        if not is_local_device(self.ips, self.port,
                               node['replication_ip'],
                               node['replication_port']):
            return False
        try:
            return check_drive(self.root, node['device'], self.mount_check)
        except ValueError:
            self.logger.warning(
                'Skipping %(device)s as it is not mounted' % node)
            return False

    def _fetch_shard_ranges(self, broker, newest=False, params=None,
                            include_deleted=False):
        path = self.int_client.make_path(broker.root_account,
                                         broker.root_container)
        params = params or {}
        params.setdefault('format', 'json')
        headers = {'X-Backend-Record-Type': 'shard',
                   'X-Backend-Override-Deleted': 'true',
                   'X-Backend-Include-Deleted': str(include_deleted)}
        if newest:
            headers['X-Newest'] = 'true'
        try:
            resp = self.int_client.make_request(
                'GET', path, headers, acceptable_statuses=(2,),
                params=params)
        except internal_client.UnexpectedResponse as err:
            self.logger.warning("Failed to get shard ranges from %s: %s",
                                quote(broker.root_path), err)
            return None
        record_type = resp.headers.get('x-backend-record-type')
        if record_type != 'shard':
            err = 'unexpected record type %r' % record_type
            self.logger.error("Failed to get shard ranges from %s: %s",
                              quote(broker.root_path), err)
            return None

        try:
            data = json.loads(resp.body)
            if not isinstance(data, list):
                raise ValueError('not a list')
            return [ShardRange.from_dict(shard_range)
                    for shard_range in data]
        except (ValueError, TypeError, KeyError) as err:
            self.logger.error(
                "Failed to get shard ranges from %s: invalid data: %r",
                quote(broker.root_path), err)
        return None

    def _put_container(self, node, part, account, container, headers, body):
        try:
            direct_put_container(node, part, account, container,
                                 conn_timeout=self.conn_timeout,
                                 response_timeout=self.node_timeout,
                                 headers=headers, contents=body)
        except DirectClientException as err:
            self.logger.warning(
                'Failed to put shard ranges to %s:%s/%s %s/%s: %s',
                node['ip'], node['port'], node['device'],
                quote(account), quote(container), err.http_status)
        except (Exception, Timeout) as err:
            self.logger.exception(
                'Failed to put shard ranges to %s:%s/%s %s/%s: %s',
                node['ip'], node['port'], node['device'],
                quote(account), quote(container), err)
        else:
            return True
        return False

    def _send_shard_ranges(self, account, container, shard_ranges,
                           headers=None):
        body = json.dumps([dict(sr, reported=0)
                           for sr in shard_ranges]).encode('ascii')
        part, nodes = self.ring.get_nodes(account, container)
        headers = headers or {}
        headers.update({'X-Backend-Record-Type': RECORD_TYPE_SHARD,
                        USE_REPLICATION_NETWORK_HEADER: 'True',
                        'User-Agent': 'container-sharder %s' % os.getpid(),
                        'X-Timestamp': Timestamp.now().normal,
                        'Content-Length': len(body),
                        'Content-Type': 'application/json'})

        pool = GreenAsyncPile(len(nodes))
        for node in nodes:
            pool.spawn(self._put_container, node, part, account,
                       container, headers, body)

        results = pool.waitall(None)
        return results.count(True) >= quorum_size(self.ring.replica_count)

    def _get_shard_broker(self, shard_range, root_path, policy_index):
        """
        Get a broker for a container db for the given shard range. If one of
        the shard container's primary nodes is a local device then that will be
        chosen for the db, otherwise the first of the shard container's handoff
        nodes that is local will be chosen.

        :param shard_range: a :class:`~swift.common.utils.ShardRange`
        :param root_path: the path of the shard's root container
        :param policy_index: the storage policy index
        :returns: a tuple of ``(part, broker, node_id, put_timestamp)`` where
            ``part`` is the shard container's partition,
            ``broker`` is an instance of
            :class:`~swift.container.backend.ContainerBroker`,
            ``node_id`` is the id of the selected node,
            ``put_timestamp`` is the put_timestamp if the broker needed to
            be initialized.
        """
        part = self.ring.get_part(shard_range.account, shard_range.container)
        node = self.find_local_handoff_for_part(part)

        put_timestamp = Timestamp.now().internal
        shard_broker, initialized = ContainerBroker.create_broker(
            os.path.join(self.root, node['device']), part, shard_range.account,
            shard_range.container, epoch=shard_range.epoch,
            storage_policy_index=policy_index, put_timestamp=put_timestamp)

        # Get the valid info into the broker.container, etc
        shard_broker.get_info()
        shard_broker.merge_shard_ranges(shard_range)
        shard_broker.set_sharding_sysmeta('Quoted-Root', quote(root_path))
        # NB: we *used* to do
        #    shard_broker.set_sharding_sysmeta('Root', root_path)
        # but that isn't safe for container names with nulls or newlines (or
        # possibly some other characters). We consciously *don't* make any
        # attempt to set the old meta; during an upgrade, some shards may think
        # they are in fact roots, but it cleans up well enough once everyone's
        # upgraded.
        shard_broker.update_metadata({
            'X-Container-Sysmeta-Sharding':
                ('True', Timestamp.now().internal)})

        put_timestamp = put_timestamp if initialized else None
        return part, shard_broker, node['id'], put_timestamp

    def _audit_root_container(self, broker):
        # This is the root container, and therefore the tome of knowledge,
        # all we can do is check there is nothing screwy with the ranges
        self._increment_stat('audit_root', 'attempted')
        warnings = []
        own_shard_range = broker.get_own_shard_range()

        if own_shard_range.state in ShardRange.SHARDING_STATES:
            shard_ranges = [sr for sr in broker.get_shard_ranges()
                            if sr.state != ShardRange.SHRINKING]
            paths_with_gaps = find_paths_with_gaps(shard_ranges)
            if paths_with_gaps:
                warnings.append(
                    'missing range(s): %s' %
                    ' '.join(['%s-%s' % (gap.lower, gap.upper)
                              for (_, gap, _) in paths_with_gaps]))

        for state in ShardRange.STATES:
            if state == ShardRange.SHRINKING:
                # Shrinking is how we resolve overlaps; we've got to
                # allow multiple shards in that state
                continue
            shard_ranges = broker.get_shard_ranges(states=state)
            # Transient overlaps can occur during the period immediately after
            # sharding if a root learns about new child shards before it learns
            # that the parent has sharded. These overlaps are normally
            # corrected as an up-to-date version of the parent shard range is
            # replicated to the root. Parent-child overlaps are therefore
            # ignored for a reclaim age after the child was created. After
            # that, parent-child overlaps may indicate that there is
            # permanently stale parent shard range data, perhaps from a node
            # that has been offline, so these are reported.
            overlaps = find_overlapping_ranges(
                shard_ranges, exclude_parent_child=True,
                time_period=self.reclaim_age)
            if overlaps:
                self._increment_stat('audit_root', 'has_overlap')
                self._update_stat('audit_root', 'num_overlap',
                                  step=len(overlaps))
                all_overlaps = ', '.join(
                    [' '.join(['%s-%s' % (sr.lower, sr.upper)
                               for sr in overlapping_ranges])
                     for overlapping_ranges in sorted(list(overlaps))])
                warnings.append(
                    'overlapping ranges in state %r: %s' %
                    (ShardRange.STATES[state], all_overlaps))

        # We've seen a case in production where the roots own_shard_range
        # epoch is reset to None, and state set to ACTIVE (like re-defaulted)
        # Epoch it important to sharding so we want to detect if this happens
        # 1. So we can alert, and 2. to see how common it is.
        if own_shard_range.epoch is None and broker.db_epoch:
            warnings.append('own_shard_range reset to None should be %s'
                            % broker.db_epoch)

        if warnings:
            self.logger.warning(
                'Audit failed for root %s (%s): %s',
                broker.db_file, quote(broker.path), ', '.join(warnings))
            self._increment_stat('audit_root', 'failure', statsd=True)
            return False

        self._increment_stat('audit_root', 'success', statsd=True)
        return True

    def _merge_shard_ranges_from_root(self, broker, shard_ranges,
                                      own_shard_range):
        """
        Merge appropriate items from the given ``shard_ranges`` into the
        ``broker``. The selection of items that are merged will depend upon the
        state of the shard.

        :param broker: A :class:`~swift.container.backend.ContainerBroker`.
        :param shard_ranges: A list of instances of
            :class:`~swift.common.utils.ShardRange` describing the shard ranges
            fetched from the root container.
        :param own_shard_range: A :class:`~swift.common.utils.ShardRange`
            describing the shard's own shard range.
        :return: a tuple of ``own_shard_range, own_shard_range_from_root``. The
            returned``own_shard_range`` will have been updated if the matching
            ``own_shard_range_from_root`` has newer data.
            ``own_shard_range_from_root`` will be None if no such matching
            shard range is found in ``shard_ranges``.
        """
        own_shard_range_from_root = None
        children_shard_ranges = []
        other_shard_ranges = []
        for shard_range in shard_ranges:
            # look for this shard range in the list of shard ranges received
            # from root; the root may have different lower and upper bounds for
            # this shard (e.g. if this shard has been expanded in the root to
            # accept a shrinking shard) so we only match on name.
            if shard_range.name == own_shard_range.name:
                # If we find our own shard range in the root response, merge
                # it and reload own shard range (note: own_range_from_root may
                # not necessarily be 'newer' than the own shard range we
                # already have, but merging will get us to the 'newest' state)
                self.logger.debug('Updating own shard range from root')
                own_shard_range_from_root = shard_range
                broker.merge_shard_ranges(own_shard_range_from_root)
                orig_own_shard_range = own_shard_range
                own_shard_range = broker.get_own_shard_range()
                if (orig_own_shard_range != own_shard_range or
                        orig_own_shard_range.state != own_shard_range.state):
                    self.logger.info('Updated own shard range from %s to %s',
                                     orig_own_shard_range, own_shard_range)
            elif shard_range.is_child_of(own_shard_range):
                children_shard_ranges.append(shard_range)
            else:
                other_shard_ranges.append(shard_range)

        if children_shard_ranges and not broker.is_sharded():
            # Merging shard ranges from the root is only necessary until this
            # DB is fully cleaved and reaches SHARDED DB state, after which it
            # is useful for debugging for the set of sub-shards to which a
            # shards has sharded to be frozen.
            self.logger.debug('Updating %d children shard ranges from root',
                              len(children_shard_ranges))
            broker.merge_shard_ranges(children_shard_ranges)

        if (other_shard_ranges
                and own_shard_range.state in ShardRange.CLEAVING_STATES
                and not broker.is_sharded()):
            # Other shard ranges returned from the root may need to be merged
            # for the purposes of sharding or shrinking this shard:
            #
            # Shrinking states: If the up-to-date state is shrinking, the
            # shards fetched from root may contain shards into which this shard
            # is to shrink itself. Shrinking is initiated by modifying multiple
            # neighboring shard range states *in the root*, rather than
            # modifying a shard directly. We therefore need to learn about
            # *other* neighboring shard ranges from the root, possibly
            # including the root itself. We need to include shrunk state too,
            # because one replica of a shard may already have moved the
            # own_shard_range state to shrunk while another replica may still
            # be in the process of shrinking.
            #
            # Sharding states: Normally a shard will shard to its own children.
            # However, in some circumstances a shard may need to shard to other
            # non-children sub-shards. For example, a shard range repair may
            # cause a child sub-shard to be deleted and its namespace covered
            # by another 'acceptor' shard.
            #
            # Therefore, if the up-to-date own_shard_range state indicates that
            # sharding or shrinking is in progress, then other shard ranges
            # will be merged, with the following caveats: we never expect a
            # shard to shard to any ancestor shard range including the root,
            # but containers might ultimately *shrink* to root; we never want
            # to cleave to a container that is itself sharding or shrinking;
            # the merged shard ranges should not result in gaps or overlaps in
            # the namespace of this shard.
            #
            # Note: the search for ancestors is guaranteed to find the parent
            # and root *if they are present*, but if any ancestor is missing
            # then there is a chance that older generations in the
            # other_shard_ranges will not be filtered and could be merged. That
            # is only a problem if they are somehow still in ACTIVE state, and
            # no overlap is detected, so the ancestor is merged.
            ancestor_names = [
                sr.name for sr in own_shard_range.find_ancestors(shard_ranges)]
            filtered_other_shard_ranges = [
                sr for sr in other_shard_ranges
                if (sr.name not in ancestor_names
                    and (sr.state not in ShardRange.CLEAVING_STATES
                         or sr.deleted))
            ]
            if own_shard_range.state in ShardRange.SHRINKING_STATES:
                root_shard_range = own_shard_range.find_root(
                    other_shard_ranges)
                if (root_shard_range and
                        root_shard_range.state == ShardRange.ACTIVE):
                    filtered_other_shard_ranges.append(root_shard_range)
            existing_shard_ranges = broker.get_shard_ranges()
            combined_shard_ranges = combine_shard_ranges(
                filtered_other_shard_ranges, existing_shard_ranges)
            overlaps = find_overlapping_ranges(combined_shard_ranges)
            paths_with_gaps = find_paths_with_gaps(
                combined_shard_ranges, own_shard_range)
            if not (overlaps or paths_with_gaps):
                # only merge if shard ranges appear to be *good*
                self.logger.debug(
                    'Updating %s other shard range(s) from root',
                    len(filtered_other_shard_ranges))
                broker.merge_shard_ranges(filtered_other_shard_ranges)

        return own_shard_range, own_shard_range_from_root

    def _delete_shard_container(self, broker, own_shard_range):
        """
        Mark a shard container as deleted if it was sharded or shrunk more than
        reclaim_age in the past. (The DB file will be removed by the replicator
        after a further reclaim_age.)

        :param broker: A :class:`~swift.container.backend.ContainerBroker`.
        :param own_shard_range: A :class:`~swift.common.utils.ShardRange`
            describing the shard's own shard range.
        """
        delete_age = time.time() - self.reclaim_age
        deletable_states = (ShardRange.SHARDED, ShardRange.SHRUNK)
        if (own_shard_range.state in deletable_states and
                own_shard_range.deleted and
                own_shard_range.timestamp < delete_age and
                broker.empty()):
            broker.delete_db(Timestamp.now().internal)
            self.logger.debug('Marked shard container as deleted %s (%s)',
                              broker.db_file, quote(broker.path))

    def _do_audit_shard_container(self, broker):
        warnings = []
        if not broker.account.startswith(self.shards_account_prefix):
            warnings.append('account not in shards namespace %r' %
                            self.shards_account_prefix)

        own_shard_range = broker.get_own_shard_range(no_default=True)

        if not own_shard_range:
            self.logger.warning('Audit failed for shard %s (%s) - skipping: '
                                'missing own shard range',
                                broker.db_file, quote(broker.path))
            return False, warnings

        # Get the root view of the world, at least that part of the world
        # that overlaps with this shard's namespace. The
        # 'states=auditing' parameter will cause the root to include
        # its own shard range in the response, which is necessary for the
        # particular case when this shard should be shrinking to the root
        # container; when not shrinking to root, but to another acceptor,
        # the root range should be in sharded state and will not interfere
        # with cleaving, listing or updating behaviour.
        shard_ranges = self._fetch_shard_ranges(
            broker, newest=True,
            params={'marker': str_to_wsgi(own_shard_range.lower_str),
                    'end_marker': str_to_wsgi(own_shard_range.upper_str),
                    'states': 'auditing'},
            include_deleted=True)
        if shard_ranges:
            own_shard_range, own_shard_range_from_root = \
                self._merge_shard_ranges_from_root(
                    broker, shard_ranges, own_shard_range)
            if not own_shard_range_from_root:
                # this is not necessarily an error - some replicas of the
                # root may not yet know about this shard container, or the
                # shard's own shard range could become deleted and
                # reclaimed from the root under rare conditions
                warnings.append('root has no matching shard range')
        elif not own_shard_range.deleted:
            warnings.append('unable to get shard ranges from root')
        # else, our shard range is deleted, so root may have reclaimed it

        self._delete_shard_container(broker, own_shard_range)

        return True, warnings

    def _audit_shard_container(self, broker):
        self._increment_stat('audit_shard', 'attempted')
        success, warnings = self._do_audit_shard_container(broker)
        if warnings:
            self.logger.warning(
                'Audit warnings for shard %s (%s): %s',
                broker.db_file, quote(broker.path), ', '.join(warnings))
        self._increment_stat(
            'audit_shard', 'success' if success else 'failure', statsd=True)
        return success

    def _audit_cleave_contexts(self, broker):
        now = Timestamp.now()
        for context, last_mod in CleavingContext.load_all(broker):
            last_mod = Timestamp(last_mod)
            is_done = context.done() and last_mod.timestamp + \
                self.recon_sharded_timeout < now.timestamp
            is_stale = last_mod.timestamp + self.reclaim_age < now.timestamp
            if is_done or is_stale:
                context.delete(broker)

    def _audit_container(self, broker):
        if broker.is_deleted():
            if broker.is_old_enough_to_reclaim(time.time(), self.reclaim_age) \
                    and not broker.is_empty_enough_to_reclaim():
                self.logger.warning(
                    'Reclaimable db stuck waiting for shrinking: %s (%s)',
                    broker.db_file, quote(broker.path))
            # if the container has been marked as deleted, all metadata will
            # have been erased so no point auditing. But we want it to pass, in
            # case any objects exist inside it.
            return True
        self._audit_cleave_contexts(broker)
        if broker.is_root_container():
            return self._audit_root_container(broker)
        return self._audit_shard_container(broker)

    def yield_objects(self, broker, src_shard_range, since_row=None):
        """
        Iterates through all objects in ``src_shard_range`` in name order
        yielding them in lists of up to CONTAINER_LISTING_LIMIT length. Both
        deleted and undeleted objects are included.

        :param broker: A :class:`~swift.container.backend.ContainerBroker`.
        :param src_shard_range: A :class:`~swift.common.utils.ShardRange`
            describing the source range.
        :param since_row: include only items whose ROWID is greater than
            the given row id; by default all rows are included.
        :return: a generator of tuples of (list of objects, broker info dict)
        """
        marker = src_shard_range.lower_str
        while True:
            info = broker.get_info()
            info['max_row'] = broker.get_max_row()
            start = time.time()
            objects = broker.get_objects(
                self.cleave_row_batch_size,
                marker=marker,
                end_marker=src_shard_range.end_marker,
                include_deleted=None,  # give me everything
                since_row=since_row)
            if objects:
                self.logger.debug('got %s objects from %s in %ss',
                                  len(objects), broker.db_file,
                                  time.time() - start)
                yield objects, info

            if len(objects) < self.cleave_row_batch_size:
                break
            marker = objects[-1]['name']

    def yield_objects_to_shard_range(self, broker, src_shard_range,
                                     dest_shard_ranges):
        """
        Iterates through all objects in ``src_shard_range`` to place them in
        destination shard ranges provided by the ``next_shard_range`` function.
        Yields tuples of (object list, destination shard range in which those
        objects belong). Note that the same destination shard range may be
        referenced in more than one yielded tuple.

        :param broker: A :class:`~swift.container.backend.ContainerBroker`.
        :param src_shard_range: A :class:`~swift.common.utils.ShardRange`
            describing the source range.
        :param dest_shard_ranges: A function which should return a list of
            destination shard ranges in name order.
        :return: a generator of tuples of
            (object list, shard range, broker info dict)
        """
        dest_shard_range_iter = dest_shard_range = None
        for objs, info in self.yield_objects(broker, src_shard_range):
            if not objs:
                return

            def next_or_none(it):
                try:
                    return next(it)
                except StopIteration:
                    return None

            if dest_shard_range_iter is None:
                dest_shard_range_iter = iter(dest_shard_ranges())
                dest_shard_range = next_or_none(dest_shard_range_iter)

            unplaced = False
            last_index = next_index = 0
            for obj in objs:
                if dest_shard_range is None:
                    # no more destinations: yield remainder of batch and bail
                    # NB there may be more batches of objects but none of them
                    # will be placed so no point fetching them
                    yield objs[last_index:], None, info
                    return
                if obj['name'] <= dest_shard_range.lower:
                    unplaced = True
                elif unplaced:
                    # end of run of unplaced objects, yield them
                    yield objs[last_index:next_index], None, info
                    last_index = next_index
                    unplaced = False
                while (dest_shard_range and
                       obj['name'] > dest_shard_range.upper):
                    if next_index != last_index:
                        # yield the objects in current dest_shard_range
                        yield (objs[last_index:next_index],
                               dest_shard_range,
                               info)
                    last_index = next_index
                    dest_shard_range = next_or_none(dest_shard_range_iter)
                next_index += 1

            if next_index != last_index:
                # yield tail of current batch of objects
                # NB there may be more objects for the current
                # dest_shard_range in the next batch from yield_objects
                yield (objs[last_index:next_index],
                       None if unplaced else dest_shard_range,
                       info)

    def _post_replicate_hook(self, broker, info, responses):
        # override superclass behaviour
        pass

    def _replicate_and_delete(self, broker, dest_shard_range, part,
                              dest_broker, node_id, info):
        success, responses = self._replicate_object(
            part, dest_broker.db_file, node_id)
        quorum = quorum_size(self.ring.replica_count)
        if not success and responses.count(True) < quorum:
            self.logger.warning(
                'Failed to sufficiently replicate misplaced objects: %s in %s '
                '(not removing)', dest_shard_range, quote(broker.path))
            return False

        if broker.get_info()['id'] != info['id']:
            # the db changed - don't remove any objects
            success = False
        else:
            # remove objects up to the max row of the db sampled prior to
            # the first object yielded for this destination; objects added
            # after that point may not have been yielded and replicated so
            # it is not safe to remove them yet
            broker.remove_objects(
                dest_shard_range.lower_str,
                dest_shard_range.upper_str,
                max_row=info['max_row'])
            success = True

        if not success:
            self.logger.warning(
                'Refused to remove misplaced objects: %s in %s',
                dest_shard_range, quote(broker.path))
        return success

    def _move_objects(self, src_broker, src_shard_range, policy_index,
                      shard_range_fetcher):
        # move objects from src_shard_range in src_broker to destination shard
        # ranges provided by shard_range_fetcher
        dest_brokers = {}  # map shard range -> broker
        placed = unplaced = 0
        success = True
        for objs, dest_shard_range, info in self.yield_objects_to_shard_range(
                src_broker, src_shard_range, shard_range_fetcher):
            if not dest_shard_range:
                unplaced += len(objs)
                success = False
                continue

            if dest_shard_range.name == src_broker.path:
                self.logger.debug(
                    'Skipping source as misplaced objects destination')
                # in shrinking context, the misplaced objects might actually be
                # correctly placed if the root has expanded this shard but this
                # broker has not yet been updated
                continue

            if dest_shard_range not in dest_brokers:
                part, dest_broker, node_id, put_timestamp = \
                    self._get_shard_broker(
                        dest_shard_range, src_broker.root_path, policy_index)
                stat = 'db_exists' if put_timestamp is None else 'db_created'
                self._increment_stat('misplaced', stat, statsd=True)
                # save the broker info that was sampled prior to the *first*
                # yielded objects for this destination
                destination = {'part': part,
                               'dest_broker': dest_broker,
                               'node_id': node_id,
                               'info': info}
                dest_brokers[dest_shard_range] = destination
            else:
                destination = dest_brokers[dest_shard_range]
            destination['dest_broker'].merge_items(objs)
            placed += len(objs)

        if unplaced:
            self.logger.warning(
                'Failed to find destination for at least %s misplaced objects '
                'in %s', unplaced, quote(src_broker.path))

        # TODO: consider executing the replication jobs concurrently
        for dest_shard_range, dest_args in dest_brokers.items():
            self.logger.debug('moving misplaced objects found in range %s' %
                              dest_shard_range)
            success &= self._replicate_and_delete(
                src_broker, dest_shard_range, **dest_args)

        self._update_stat('misplaced', 'placed', step=placed, statsd=True)
        self._update_stat('misplaced', 'unplaced', step=unplaced, statsd=True)
        return success, placed, unplaced

    def _make_shard_range_fetcher(self, broker, src_shard_range):
        # returns a function that will lazy load shard ranges on demand;
        # this means only one lookup is made for all misplaced ranges.
        outer = {}

        def shard_range_fetcher():
            if not outer:
                if broker.is_root_container():
                    ranges = broker.get_shard_ranges(
                        marker=src_shard_range.lower_str,
                        end_marker=src_shard_range.end_marker,
                        states=SHARD_UPDATE_STATES)
                else:
                    # TODO: the root may not yet know about shard ranges to
                    # which a shard is sharding, but those could come from
                    # the broker
                    ranges = self._fetch_shard_ranges(
                        broker, newest=True,
                        params={'states': 'updating',
                                'marker': str_to_wsgi(
                                    src_shard_range.lower_str),
                                'end_marker': str_to_wsgi(
                                    src_shard_range.end_marker)})
                outer['ranges'] = iter(ranges)
            return outer['ranges']
        return shard_range_fetcher

    def _make_default_misplaced_object_bounds(self, broker):
        # Objects outside of this container's own range are misplaced.
        own_shard_range = broker.get_own_shard_range()
        bounds = []
        if own_shard_range.lower:
            bounds.append(('', own_shard_range.lower))
        if own_shard_range.upper:
            bounds.append((own_shard_range.upper, ''))
        return bounds

    def _make_misplaced_object_bounds(self, broker):
        bounds = []
        state = broker.get_db_state()
        if state == SHARDED:
            # Anything in the object table is treated as a misplaced object.
            bounds.append(('', ''))

        if not bounds and state == SHARDING:
            # Objects outside of this container's own range are misplaced.
            # Objects in already cleaved shard ranges are also misplaced.
            cleave_context = CleavingContext.load(broker)
            if cleave_context.cursor:
                bounds.append(('', cleave_context.cursor))
                own_shard_range = broker.get_own_shard_range()
                if own_shard_range.upper:
                    bounds.append((own_shard_range.upper, ''))

        return bounds or self._make_default_misplaced_object_bounds(broker)

    def _move_misplaced_objects(self, broker, src_broker=None,
                                src_bounds=None):
        """
        Search for objects in the given broker that do not belong in that
        broker's namespace and move those objects to their correct shard
        container.

        :param broker: An instance of :class:`swift.container.ContainerBroker`.
        :param src_broker: optional alternative broker to use as the source
            of misplaced objects; if not specified then ``broker`` is used as
            the source.
        :param src_bounds: optional list of (lower, upper) namespace bounds to
            use when searching for misplaced objects
        :return: True if all misplaced objects were sufficiently replicated to
            their correct shard containers, False otherwise
        """
        self.logger.debug('Looking for misplaced objects in %s (%s)',
                          quote(broker.path), broker.db_file)
        self._increment_stat('misplaced', 'attempted')
        src_broker = src_broker or broker
        if src_bounds is None:
            src_bounds = self._make_misplaced_object_bounds(broker)
        # (ab)use ShardRange instances to encapsulate source namespaces
        src_ranges = [ShardRange('dont/care', Timestamp.now(), lower, upper)
                      for lower, upper in src_bounds]
        self.logger.debug('misplaced object source bounds %s' % src_bounds)
        policy_index = broker.storage_policy_index
        success = True
        num_placed = num_unplaced = 0
        for src_shard_range in src_ranges:
            part_success, part_placed, part_unplaced = self._move_objects(
                src_broker, src_shard_range, policy_index,
                self._make_shard_range_fetcher(broker, src_shard_range))
            success &= part_success
            num_placed += part_placed
            num_unplaced += part_unplaced

        if num_placed or num_unplaced:
            # the found stat records the number of DBs in which any misplaced
            # rows were found, not the total number of misplaced rows
            self._increment_stat('misplaced', 'found', statsd=True)
            self.logger.debug('Placed %s misplaced objects (%s unplaced)',
                              num_placed, num_unplaced)
        self._increment_stat('misplaced', 'success' if success else 'failure',
                             statsd=True)
        self.logger.debug('Finished handling misplaced objects')
        return success

    def _find_shard_ranges(self, broker):
        """
        Scans the container to find shard ranges and adds them to the shard
        ranges table. If there are existing shard ranges then scanning starts
        from the upper bound of the uppermost existing shard range.

        :param broker: An instance of :class:`swift.container.ContainerBroker`
        :return: a tuple of (success, num of shard ranges found) where success
            is True if the last shard range has been found, False otherwise.
        """
        own_shard_range = broker.get_own_shard_range()
        shard_ranges = broker.get_shard_ranges()
        if shard_ranges and shard_ranges[-1].upper >= own_shard_range.upper:
            self.logger.debug('Scan for shard ranges already completed for %s',
                              quote(broker.path))
            return 0

        self.logger.info('Starting scan for shard ranges on %s',
                         quote(broker.path))
        self._increment_stat('scanned', 'attempted')

        start = time.time()
        shard_data, last_found = broker.find_shard_ranges(
            self.rows_per_shard, limit=self.shard_scanner_batch_size,
            existing_ranges=shard_ranges,
            minimum_shard_size=self.minimum_shard_size)
        elapsed = time.time() - start

        if not shard_data:
            if last_found:
                self.logger.info("Already found all shard ranges")
                self._increment_stat('scanned', 'success', statsd=True)
            else:
                # we didn't find anything
                self.logger.warning("No shard ranges found")
                self._increment_stat('scanned', 'failure', statsd=True)
            return 0

        shard_ranges = make_shard_ranges(
            broker, shard_data, self.shards_account_prefix)
        broker.merge_shard_ranges(shard_ranges)
        num_found = len(shard_ranges)
        self.logger.info(
            "Completed scan for shard ranges: %d found", num_found)
        self._update_stat('scanned', 'found', step=num_found)
        self._min_stat('scanned', 'min_time', round(elapsed / num_found, 3))
        self._max_stat('scanned', 'max_time', round(elapsed / num_found, 3))

        if last_found:
            self.logger.info("Final shard range reached.")
        self._increment_stat('scanned', 'success', statsd=True)
        return num_found

    def _create_shard_containers(self, broker):
        # Create shard containers that are ready to receive redirected object
        # updates. Do this now, so that redirection can begin immediately
        # without waiting for cleaving to complete.
        found_ranges = broker.get_shard_ranges(states=ShardRange.FOUND)
        created_ranges = []
        for shard_range in found_ranges:
            self._increment_stat('created', 'attempted')
            shard_range.update_state(ShardRange.CREATED)
            headers = {
                'X-Backend-Storage-Policy-Index': broker.storage_policy_index,
                'X-Container-Sysmeta-Shard-Quoted-Root': quote(
                    broker.root_path),
                'X-Container-Sysmeta-Sharding': 'True',
                'X-Backend-Auto-Create': 'True'}
            # NB: we *used* to send along
            #    'X-Container-Sysmeta-Shard-Root': broker.root_path
            # but that isn't safe for container names with nulls or newlines
            # (or possibly some other characters). We consciously *don't* make
            # any attempt to set the old meta; during an upgrade, some shards
            # may think they are in fact roots, but it cleans up well enough
            # once everyone's upgraded.
            success = self._send_shard_ranges(
                shard_range.account, shard_range.container,
                [shard_range], headers=headers)
            if success:
                self.logger.debug('PUT new shard range container for %s',
                                  shard_range)
                self._increment_stat('created', 'success', statsd=True)
            else:
                self.logger.error(
                    'PUT of new shard container %r failed for %s.',
                    shard_range, quote(broker.path))
                self._increment_stat('created', 'failure', statsd=True)
                # break, not continue, because elsewhere it is assumed that
                # finding and cleaving shard ranges progresses linearly, so we
                # do not want any subsequent shard ranges to be in created
                # state while this one is still in found state
                break
            created_ranges.append(shard_range)

        if created_ranges:
            broker.merge_shard_ranges(created_ranges)
            if not broker.is_root_container():
                self._send_shard_ranges(
                    broker.root_account, broker.root_container, created_ranges)
            self.logger.info(
                "Completed creating shard range containers: %d created, "
                "from sharding container %s",
                len(created_ranges), quote(broker.path))
        return len(created_ranges)

    def _cleave_shard_broker(self, broker, cleaving_context, shard_range,
                             own_shard_range, shard_broker, put_timestamp,
                             shard_part, node_id):
        result = CLEAVE_SUCCESS
        start = time.time()
        # only cleave from the retiring db - misplaced objects handler will
        # deal with any objects in the fresh db
        source_broker = broker.get_brokers()[0]
        # if this range has been cleaved before but replication
        # failed then the shard db may still exist and it may not be
        # necessary to merge all the rows again
        source_db_id = source_broker.get_info()['id']
        source_max_row = source_broker.get_max_row()
        sync_point = shard_broker.get_sync(source_db_id)
        if sync_point < source_max_row or source_max_row == -1:
            sync_from_row = max(cleaving_context.last_cleave_to_row or -1,
                                sync_point)
            objects = None
            for objects, info in self.yield_objects(
                    source_broker, shard_range,
                    since_row=sync_from_row):
                shard_broker.merge_items(objects)
            if objects is None:
                self.logger.info("Cleaving '%s': %r - zero objects found",
                                 quote(broker.path), shard_range)
                if shard_broker.get_info()['put_timestamp'] == put_timestamp:
                    # This was just created; don't need to replicate this
                    # SR because there was nothing there. So cleanup and
                    # remove the shard_broker from its hand off location.
                    # Because nothing was here we wont count it in the shard
                    # batch count.
                    result = CLEAVE_EMPTY
                # Else, it wasn't newly created by us, and
                # we don't know what's in it or why. Let it get
                # replicated and counted in the batch count.

            # Note: the max row stored as a sync point is sampled *before*
            # objects are yielded to ensure that is less than or equal to
            # the last yielded row. Other sync points are also copied from the
            # source broker to the shards; if another replica of the source
            # happens to subsequently cleave into a primary replica of the
            # shard then it will only need to cleave rows after its last sync
            # point with this replica of the source broker.
            shard_broker.merge_syncs(
                [{'sync_point': source_max_row, 'remote_id': source_db_id}] +
                source_broker.get_syncs())
        else:
            self.logger.debug("Cleaving '%s': %r - shard db already in sync",
                              quote(broker.path), shard_range)

        replication_quorum = self.existing_shard_replication_quorum
        if own_shard_range.state in ShardRange.SHRINKING_STATES:
            if shard_range.includes(own_shard_range):
                # When shrinking to a single acceptor that completely encloses
                # this shard's namespace, include deleted own (donor) shard
                # range in the replicated db so that when acceptor next updates
                # root it will atomically update its namespace *and* delete the
                # donor. This reduces the chance of a temporary listing gap if
                # this shard fails to update the root with its SHRUNK/deleted
                # state. Don't do this when sharding a shard or shrinking to
                # multiple acceptors because in those cases the donor namespace
                # should not be deleted until *all* shards are cleaved.
                if own_shard_range.update_state(ShardRange.SHRUNK):
                    own_shard_range.set_deleted()
                    broker.merge_shard_ranges(own_shard_range)
                shard_broker.merge_shard_ranges(own_shard_range)
        elif shard_range.state == ShardRange.CREATED:
            # The shard range object stats may have changed since the shard
            # range was found, so update with stats of objects actually
            # copied to the shard broker. Only do this the first time each
            # shard range is cleaved.
            info = shard_broker.get_info()
            shard_range.update_meta(
                info['object_count'], info['bytes_used'])
            # Update state to CLEAVED; only do this when sharding, not when
            # shrinking
            shard_range.update_state(ShardRange.CLEAVED)
            shard_broker.merge_shard_ranges(shard_range)
            replication_quorum = self.shard_replication_quorum

        if result == CLEAVE_EMPTY:
            self.delete_db(shard_broker)
        else:  # result == CLEAVE_SUCCESS:
            self.logger.info(
                'Replicating new shard container %s for %s',
                quote(shard_broker.path), own_shard_range)

            success, responses = self._replicate_object(
                shard_part, shard_broker.db_file, node_id)

            replication_successes = responses.count(True)
            if (not success and (not responses or
                                 replication_successes < replication_quorum)):
                # insufficient replication or replication not even attempted;
                # break because we don't want to progress the cleave cursor
                # until each shard range has been successfully cleaved
                self.logger.warning(
                    'Failed to sufficiently replicate cleaved shard %s for %s:'
                    ' %s successes, %s required.', shard_range,
                    quote(broker.path),
                    replication_successes, replication_quorum)
                self._increment_stat('cleaved', 'failure', statsd=True)
                result = CLEAVE_FAILED
            else:
                elapsed = round(time.time() - start, 3)
                self._min_stat('cleaved', 'min_time', elapsed)
                self._max_stat('cleaved', 'max_time', elapsed)
                self.logger.info(
                    'Cleaved %s for shard range %s in %gs.',
                    quote(broker.path), shard_range, elapsed)
                self._increment_stat('cleaved', 'success', statsd=True)

        if result in (CLEAVE_SUCCESS, CLEAVE_EMPTY):
            broker.merge_shard_ranges(shard_range)
            cleaving_context.range_done(shard_range.upper_str)
            if shard_range.upper >= own_shard_range.upper:
                # cleaving complete
                cleaving_context.cleaving_done = True
            cleaving_context.store(broker)
        return result

    def _cleave_shard_range(self, broker, cleaving_context, shard_range,
                            own_shard_range):
        self.logger.info("Cleaving '%s' from row %s into %s for %r",
                         quote(broker.path),
                         cleaving_context.last_cleave_to_row,
                         quote(shard_range.name), shard_range)
        self._increment_stat('cleaved', 'attempted')
        policy_index = broker.storage_policy_index
        shard_part, shard_broker, node_id, put_timestamp = \
            self._get_shard_broker(shard_range, broker.root_path,
                                   policy_index)
        stat = 'db_exists' if put_timestamp is None else 'db_created'
        self._increment_stat('cleaved', stat, statsd=True)
        return self._cleave_shard_broker(
            broker, cleaving_context, shard_range, own_shard_range,
            shard_broker, put_timestamp, shard_part, node_id)

    def _cleave(self, broker):
        # Returns True if misplaced objects have been moved and the entire
        # container namespace has been successfully cleaved, False otherwise
        if broker.is_sharded():
            self.logger.debug('Passing over already sharded container %s',
                              quote(broker.path))
            return True

        cleaving_context = CleavingContext.load(broker)
        if not cleaving_context.misplaced_done:
            # ensure any misplaced objects in the source broker are moved; note
            # that this invocation of _move_misplaced_objects is targetted at
            # the *retiring* db.
            self.logger.debug(
                'Moving any misplaced objects from sharding container: %s',
                quote(broker.path))
            bounds = self._make_default_misplaced_object_bounds(broker)
            cleaving_context.misplaced_done = self._move_misplaced_objects(
                broker, src_broker=broker.get_brokers()[0],
                src_bounds=bounds)
            cleaving_context.store(broker)

        if cleaving_context.cleaving_done:
            self.logger.debug('Cleaving already complete for container %s',
                              quote(broker.path))
            return cleaving_context.misplaced_done

        shard_ranges = broker.get_shard_ranges(marker=cleaving_context.marker)
        # Ignore shrinking shard ranges: we never want to cleave objects to a
        # shrinking shard. Shrinking shard ranges are to be expected in a root;
        # shrinking shard ranges (other than own shard range) are not normally
        # expected in a shard but can occur if there is an overlapping shard
        # range that has been discovered from the root.
        ranges_todo = [sr for sr in shard_ranges
                       if sr.state != ShardRange.SHRINKING]
        if cleaving_context.cursor:
            # always update ranges_todo in case shard ranges have changed since
            # last visit
            cleaving_context.ranges_todo = len(ranges_todo)
            self.logger.debug('Continuing to cleave (%s done, %s todo): %s',
                              cleaving_context.ranges_done,
                              cleaving_context.ranges_todo,
                              quote(broker.path))
        else:
            cleaving_context.start()
            own_shard_range = broker.get_own_shard_range()
            cleaving_context.cursor = own_shard_range.lower_str
            cleaving_context.ranges_todo = len(ranges_todo)
            self.logger.info('Starting to cleave (%s todo): %s',
                             cleaving_context.ranges_todo, quote(broker.path))

        own_shard_range = broker.get_own_shard_range(no_default=True)
        if own_shard_range is None:
            # A default should never be SHRINKING or SHRUNK but because we
            # may write own_shard_range back to broker, let's make sure
            # it can't be defaulted.
            self.logger.warning('Failed to get own_shard_range for %s',
                                quote(broker.path))
            ranges_todo = []  # skip cleaving

        ranges_done = []
        for shard_range in ranges_todo:
            if cleaving_context.cleaving_done:
                # note: there may still be ranges_todo, for example: if this
                # shard is shrinking and has merged a root shard range in
                # sharded state along with an active acceptor shard range, but
                # the root range is irrelevant
                break

            if len(ranges_done) == self.cleave_batch_size:
                break

            if shard_range.lower > cleaving_context.cursor:
                self.logger.info('Stopped cleave at gap: %r - %r' %
                                 (cleaving_context.cursor, shard_range.lower))
                break

            if shard_range.state not in (ShardRange.CREATED,
                                         ShardRange.CLEAVED,
                                         ShardRange.ACTIVE):
                self.logger.info('Stopped cleave at unready %s', shard_range)
                break

            cleave_result = self._cleave_shard_range(
                broker, cleaving_context, shard_range, own_shard_range)

            if cleave_result == CLEAVE_SUCCESS:
                ranges_done.append(shard_range)
            elif cleave_result == CLEAVE_FAILED:
                break
            # else: CLEAVE_EMPTY: no errors, but no rows found either. keep
            # going, and don't count it against our batch size

        # _cleave_shard_range always store()s the context on success; *also* do
        # that here in case we hit a failure right off the bat or ended loop
        # with skipped ranges
        cleaving_context.store(broker)
        self.logger.debug(
            'Cleaved %s shard ranges for %s',
            len(ranges_done), quote(broker.path))
        return (cleaving_context.misplaced_done and
                cleaving_context.cleaving_done)

    def _complete_sharding(self, broker):
        cleaving_context = CleavingContext.load(broker)
        if cleaving_context.done():
            # Move all CLEAVED shards to ACTIVE state and if a shard then
            # delete own shard range; these changes will be simultaneously
            # reported in the next update to the root container.
            own_shard_range = broker.get_own_shard_range(no_default=True)
            if own_shard_range is None:
                # This is more of a belts and braces, not sure we could even
                # get this far with without an own_shard_range. But because
                # we will be writing own_shard_range back, we need to make sure
                self.logger.warning('Failed to get own_shard_range for %s',
                                    quote(broker.path))
                return False
            own_shard_range.update_meta(0, 0)
            if own_shard_range.state in ShardRange.SHRINKING_STATES:
                own_shard_range.update_state(ShardRange.SHRUNK)
                modified_shard_ranges = []
            else:
                own_shard_range.update_state(ShardRange.SHARDED)
                modified_shard_ranges = broker.get_shard_ranges(
                    states=ShardRange.CLEAVED)
                for sr in modified_shard_ranges:
                    sr.update_state(ShardRange.ACTIVE)
            if (not broker.is_root_container() and not
                    own_shard_range.deleted):
                own_shard_range = own_shard_range.copy(
                    timestamp=Timestamp.now(), deleted=1)
            modified_shard_ranges.append(own_shard_range)
            broker.merge_shard_ranges(modified_shard_ranges)
            if broker.set_sharded_state():
                return True
            else:
                self.logger.warning(
                    'Failed to remove retiring db file for %s',
                    quote(broker.path))
        else:
            self.logger.warning(
                'Repeat cleaving required for %r with context: %s',
                broker.db_files[0], dict(cleaving_context))
            cleaving_context.reset()
            cleaving_context.store(broker)

        return False

    def _find_and_enable_sharding_candidates(self, broker, shard_ranges=None):
        candidates = find_sharding_candidates(
            broker, self.shard_container_threshold, shard_ranges)
        if candidates:
            self.logger.debug('Identified %s sharding candidates',
                              len(candidates))
            broker.merge_shard_ranges(candidates)

    def _find_and_enable_shrinking_candidates(self, broker):
        if not broker.is_sharded():
            self.logger.warning('Cannot shrink a not yet sharded container %s',
                                quote(broker.path))
            return

        compactible_sequences = find_compactible_shard_sequences(
            broker, self.shrink_threshold, self.expansion_limit,
            self.max_shrinking, self.max_expanding, include_shrinking=True)
        self.logger.debug('Found %s compactible sequences of length(s) %s' %
                          (len(compactible_sequences),
                           [len(s) for s in compactible_sequences]))
        process_compactible_shard_sequences(broker, compactible_sequences)
        own_shard_range = broker.get_own_shard_range()
        for sequence in compactible_sequences:
            acceptor = sequence[-1]
            donors = ShardRangeList(sequence[:-1])
            self.logger.debug(
                'shrinking %d objects from %d shard ranges into %s in %s' %
                (donors.object_count, len(donors), acceptor, broker.db_file))
            if acceptor.name != own_shard_range.name:
                self._send_shard_ranges(
                    acceptor.account, acceptor.container, [acceptor])
                acceptor.increment_meta(donors.object_count, donors.bytes_used)
            # Now send a copy of the expanded acceptor, with an updated
            # timestamp, to each donor container. This forces each donor to
            # asynchronously cleave its entire contents to the acceptor and
            # delete itself. The donor will pass its own deleted shard range to
            # the acceptor when cleaving. Subsequent updates from the donor or
            # the acceptor will then update the root to have the  deleted donor
            # shard range.
            for donor in donors:
                self._send_shard_ranges(
                    donor.account, donor.container, [donor, acceptor])

    def _update_root_container(self, broker):
        own_shard_range = broker.get_own_shard_range(no_default=True)
        if not own_shard_range:
            return

        # do a reclaim *now* in order to get best estimate of tombstone count
        # that is consistent with the current object_count
        reclaimer = self._reclaim(broker)
        tombstones = reclaimer.get_tombstone_count()
        self.logger.debug('tombstones in %s = %d',
                          quote(broker.path), tombstones)
        own_shard_range.update_tombstones(tombstones)
        update_own_shard_range_stats(broker, own_shard_range)
        if own_shard_range.reported:
            # no change to the stats metadata
            return

        # stats metadata has been updated so persist it
        broker.merge_shard_ranges(own_shard_range)
        # now get a consistent list of own and other shard ranges
        shard_ranges = broker.get_shard_ranges(
            include_own=True,
            include_deleted=True)
        # send everything
        if self._send_shard_ranges(
                broker.root_account, broker.root_container, shard_ranges,
                {'Referer': quote(broker.path)}):
            # on success, mark ourselves as reported so we don't keep
            # hammering the root
            own_shard_range.reported = True
            broker.merge_shard_ranges(own_shard_range)
            self.logger.debug(
                'updated root objs=%d, tombstones=%s (%s)',
                own_shard_range.object_count, own_shard_range.tombstones,
                quote(broker.path))

    def _process_broker(self, broker, node, part):
        broker.get_info()  # make sure account/container are populated
        state = broker.get_db_state()
        is_deleted = broker.is_deleted()
        self.logger.debug('Starting processing %s state %s%s',
                          quote(broker.path), state,
                          ' (deleted)' if is_deleted else '')

        if not self._audit_container(broker):
            return

        # now look and deal with misplaced objects.
        self._move_misplaced_objects(broker)

        is_leader = node['index'] == 0 and self.auto_shard and not is_deleted
        if state in (UNSHARDED, COLLAPSED):
            if is_leader and broker.is_root_container():
                # bootstrap sharding of root container
                own_shard_range = broker.get_own_shard_range()
                update_own_shard_range_stats(broker, own_shard_range)
                self._find_and_enable_sharding_candidates(
                    broker, shard_ranges=[own_shard_range])

            own_shard_range = broker.get_own_shard_range()
            if own_shard_range.state in ShardRange.CLEAVING_STATES:
                if broker.get_shard_ranges():
                    # container has been given shard ranges rather than
                    # found them e.g. via replication or a shrink event,
                    # or manually triggered cleaving.
                    if broker.set_sharding_state():
                        state = SHARDING
                        self.logger.info('Kick off container cleaving on %s, '
                                         'own shard range in state %r',
                                         quote(broker.path),
                                         own_shard_range.state_text)
                elif is_leader:
                    if broker.set_sharding_state():
                        state = SHARDING
                else:
                    self.logger.debug(
                        'Own shard range in state %r but no shard ranges '
                        'and not leader; remaining unsharded: %s',
                        own_shard_range.state_text, quote(broker.path))

        if state == SHARDING:
            if is_leader:
                num_found = self._find_shard_ranges(broker)
            else:
                num_found = 0

            # create shard containers for newly found ranges
            num_created = self._create_shard_containers(broker)

            if num_found or num_created:
                # share updated shard range state with  other nodes
                self._replicate_object(part, broker.db_file, node['id'])

            # always try to cleave any pending shard ranges
            cleave_complete = self._cleave(broker)

            if cleave_complete:
                if self._complete_sharding(broker):
                    state = SHARDED
                    self._increment_stat('visited', 'completed', statsd=True)
                    self.logger.info(
                        'Completed cleaving of %s, DB set to sharded state',
                        quote(broker.path))
                else:
                    self.logger.info(
                        'Completed cleaving of %s, DB remaining in sharding '
                        'state', quote(broker.path))

        if not broker.is_deleted():
            if state == SHARDED and broker.is_root_container():
                # look for shrink stats
                self._identify_shrinking_candidate(broker, node)
                if is_leader:
                    self._find_and_enable_shrinking_candidates(broker)
                    self._find_and_enable_sharding_candidates(broker)
                for shard_range in broker.get_shard_ranges(
                        states=[ShardRange.SHARDING]):
                    self._send_shard_ranges(
                        shard_range.account, shard_range.container,
                        [shard_range])

            if not broker.is_root_container():
                # Update the root container with this container's shard range
                # info; do this even when sharded in case previous attempts
                # failed; don't do this if there is no own shard range. When
                # sharding a shard, this is when the root will see the new
                # shards move to ACTIVE state and the sharded shard
                # simultaneously become deleted.
                self._update_root_container(broker)

        self.logger.debug('Finished processing %s state %s%s',
                          quote(broker.path), broker.get_db_state(),
                          ' (deleted)' if is_deleted else '')

    def _one_shard_cycle(self, devices_to_shard, partitions_to_shard):
        """
        The main function, everything the sharder does forks from this method.

        The sharder loops through each container with sharding enabled and each
        sharded container on the server, on each container it:
            - audits the container
            - checks and deals with misplaced items
            - cleaves any shard ranges as required
            - if not a root container, reports shard range stats to the root
              container
        """

        self.logger.info('Container sharder cycle starting, auto-sharding %s',
                         self.auto_shard)
        if isinstance(devices_to_shard, (list, tuple)):
            self.logger.info('(Override devices: %s)',
                             ', '.join(str(d) for d in devices_to_shard))
        if isinstance(partitions_to_shard, (list, tuple)):
            self.logger.info('(Override partitions: %s)',
                             ', '.join(str(p) for p in partitions_to_shard))
        self._zero_stats()
        self._local_device_ids = {}
        dirs = []
        self.ips = whataremyips(self.bind_ip)
        for node in self.ring.devs:
            device_path = self._check_node(node)
            if not device_path:
                continue
            datadir = os.path.join(device_path, self.datadir)
            if os.path.isdir(datadir):
                # Populate self._local_device_ids so we can find devices for
                # shard containers later
                self._local_device_ids[node['id']] = node
                if node['device'] not in devices_to_shard:
                    continue
                part_filt = self._partition_dir_filter(
                    node['id'],
                    partitions_to_shard)
                dirs.append((datadir, node, part_filt))
        if not dirs:
            self.logger.info('Found no containers directories')
        for part, path, node in self.roundrobin_datadirs(dirs):
            # NB: get_part_nodes always provides an 'index' key;
            # this will be used in leader selection
            for primary in self.ring.get_part_nodes(int(part)):
                if node['id'] == primary['id']:
                    node = primary
                    break
            else:
                # Set index such that we'll *never* be selected as a leader
                node['index'] = 'handoff'

            broker = ContainerBroker(path, logger=self.logger,
                                     timeout=self.broker_timeout)
            error = None
            try:
                self._identify_sharding_candidate(broker, node)
                if sharding_enabled(broker):
                    self._increment_stat('visited', 'attempted')
                    self._process_broker(broker, node, part)
                    self._increment_stat('visited', 'success', statsd=True)
                else:
                    self._increment_stat('visited', 'skipped')
            except (Exception, Timeout) as err:
                self._increment_stat('visited', 'failure', statsd=True)
                self.logger.exception(
                    'Unhandled exception while processing %s: %s', path, err)
                error = err
            try:
                self._record_sharding_progress(broker, node, error)
            except (Exception, Timeout) as error:
                self.logger.exception(
                    'Unhandled exception while dumping progress for %s: %s',
                    path, error)
            self._periodic_report_stats()

        self._report_stats()

    @contextmanager
    def _set_auto_shard_from_command_line(self, **kwargs):
        conf_auto_shard = self.auto_shard
        auto_shard = kwargs.get('auto_shard', None)
        if auto_shard is not None:
            self.auto_shard = config_true_value(auto_shard)
        try:
            yield
        finally:
            self.auto_shard = conf_auto_shard

    def run_forever(self, *args, **kwargs):
        """Run the container sharder until stopped."""
        with self._set_auto_shard_from_command_line(**kwargs):
            self.reported = time.time()
            time.sleep(random() * self.interval)
            while True:
                begin = time.time()
                try:
                    self._one_shard_cycle(devices_to_shard=Everything(),
                                          partitions_to_shard=Everything())
                except (Exception, Timeout):
                    self.logger.increment('errors')
                    self.logger.exception('Exception in sharder')
                elapsed = time.time() - begin
                self.logger.info(
                    'Container sharder cycle completed: %.02fs', elapsed)
                if elapsed < self.interval:
                    time.sleep(self.interval - elapsed)

    def run_once(self, *args, **kwargs):
        """Run the container sharder once."""
        self.logger.info('Begin container sharder "once" mode')
        override_options = parse_override_options(once=True, **kwargs)
        devices_to_shard = override_options.devices or Everything()
        partitions_to_shard = override_options.partitions or Everything()
        with self._set_auto_shard_from_command_line(**kwargs):
            begin = self.reported = time.time()
            self._one_shard_cycle(devices_to_shard=devices_to_shard,
                                  partitions_to_shard=partitions_to_shard)
            elapsed = time.time() - begin
            self.logger.info(
                'Container sharder "once" mode completed: %.02fs', elapsed)
