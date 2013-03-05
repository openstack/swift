from collections import defaultdict


def tiers_for_dev(dev):
    """
    Returns a tuple of tiers for a given device in ascending order by
    length.

    :returns: tuple of tiers
    """
    t1 = dev['region']
    t2 = dev['zone']
    t3 = "{ip}:{port}".format(ip=dev.get('ip'), port=dev.get('port'))
    t4 = dev['id']

    return ((t1,),
            (t1, t2),
            (t1, t2, t3),
            (t1, t2, t3, t4))


def build_tier_tree(devices):
    """
    Construct the tier tree from the zone layout.

    The tier tree is a dictionary that maps tiers to their child tiers.
    A synthetic root node of () is generated so that there's one tree,
    not a forest.

    Example:

    region 1 -+---- zone 1 -+---- 192.168.101.1:6000 -+---- device id 0
              |             |                         |
              |             |                         +---- device id 1
              |             |                         |
              |             |                         +---- device id 2
              |             |
              |             +---- 192.168.101.2:6000 -+---- device id 3
              |                                       |
              |                                       +---- device id 4
              |                                       |
              |                                       +---- device id 5
              |
              +---- zone 2 -+---- 192.168.102.1:6000 -+---- device id 6
                            |                         |
                            |                         +---- device id 7
                            |                         |
                            |                         +---- device id 8
                            |
                            +---- 192.168.102.2:6000 -+---- device id 9
                                                      |
                                                      +---- device id 10


    region 2 -+---- zone 1 -+---- 192.168.201.1:6000 -+---- device id 12
                            |                         |
                            |                         +---- device id 13
                            |                         |
                            |                         +---- device id 14
                            |
                            +---- 192.168.201.2:6000 -+---- device id 15
                                                      |
                                                      +---- device id 16
                                                      |
                                                      +---- device id 17

    The tier tree would look like:
    {
      (): [(1,), (2,)],

      (1,): [(1, 1), (1, 2)],
      (2,): [(2, 1)],

      (1, 1): [(1, 1, 192.168.101.1:6000),
               (1, 1, 192.168.101.2:6000)],
      (1, 2): [(1, 2, 192.168.102.1:6000),
               (1, 2, 192.168.102.2:6000)],
      (2, 1): [(2, 1, 192.168.201.1:6000),
               (2, 1, 192.168.201.2:6000)],

      (1, 1, 192.168.101.1:6000): [(1, 1, 192.168.101.1:6000, 0),
                                   (1, 1, 192.168.101.1:6000, 1),
                                   (1, 1, 192.168.101.1:6000, 2)],
      (1, 1, 192.168.101.2:6000): [(1, 1, 192.168.101.2:6000, 3),
                                   (1, 1, 192.168.101.2:6000, 4),
                                   (1, 1, 192.168.101.2:6000, 5)],
      (1, 2, 192.168.102.1:6000): [(1, 2, 192.168.102.1:6000, 6),
                                   (1, 2, 192.168.102.1:6000, 7),
                                   (1, 2, 192.168.102.1:6000, 8)],
      (1, 2, 192.168.102.2:6000): [(1, 2, 192.168.102.2:6000, 9),
                                   (1, 2, 192.168.102.2:6000, 10)],
      (2, 1, 192.168.201.1:6000): [(2, 1, 192.168.201.1:6000, 12),
                                   (2, 1, 192.168.201.1:6000, 13),
                                   (2, 1, 192.168.201.1:6000, 14)],
      (2, 1, 192.168.201.2:6000): [(2, 1, 192.168.201.2:6000, 15),
                                   (2, 1, 192.168.201.2:6000, 16),
                                   (2, 1, 192.168.201.2:6000, 17)],
    }

    :devices: device dicts from which to generate the tree
    :returns: tier tree

    """
    tier2children = defaultdict(set)
    for dev in devices:
        for tier in tiers_for_dev(dev):
            if len(tier) > 1:
                tier2children[tier[0:-1]].add(tier)
            else:
                tier2children[()].add(tier)
    return tier2children
