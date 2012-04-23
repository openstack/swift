from collections import defaultdict


def tiers_for_dev(dev):
    """
    Returns a tuple of tiers for a given device in ascending order by
    length.

    :returns: tuple of tiers
    """
    t1 = dev['zone']
    t2 = "{ip}:{port}".format(ip=dev.get('ip'), port=dev.get('port'))
    t3 = dev['id']

    return ((t1,),
            (t1, t2),
            (t1, t2, t3))


def build_tier_tree(devices):
    """
    Construct the tier tree from the zone layout.

    The tier tree is a dictionary that maps tiers to their child tiers.
    A synthetic root node of () is generated so that there's one tree,
    not a forest.

    Example:

    zone 1 -+---- 192.168.1.1:6000 -+---- device id 0
            |                       |
            |                       +---- device id 1
            |                       |
            |                       +---- device id 2
            |
            +---- 192.168.1.2:6000 -+---- device id 3
                                    |
                                    +---- device id 4
                                    |
                                    +---- device id 5


    zone 2 -+---- 192.168.2.1:6000 -+---- device id 6
            |                       |
            |                       +---- device id 7
            |                       |
            |                       +---- device id 8
            |
            +---- 192.168.2.2:6000 -+---- device id 9
                                    |
                                    +---- device id 10
                                    |
                                    +---- device id 11

    The tier tree would look like:
    {
      (): [(1,), (2,)],

      (1,): [(1, 192.168.1.1:6000),
             (1, 192.168.1.2:6000)],
      (2,): [(1, 192.168.2.1:6000),
             (1, 192.168.2.2:6000)],

      (1, 192.168.1.1:6000): [(1, 192.168.1.1:6000, 0),
                              (1, 192.168.1.1:6000, 1),
                              (1, 192.168.1.1:6000, 2)],
      (1, 192.168.1.2:6000): [(1, 192.168.1.2:6000, 3),
                              (1, 192.168.1.2:6000, 4),
                              (1, 192.168.1.2:6000, 5)],
      (2, 192.168.2.1:6000): [(1, 192.168.2.1:6000, 6),
                              (1, 192.168.2.1:6000, 7),
                              (1, 192.168.2.1:6000, 8)],
      (2, 192.168.2.2:6000): [(1, 192.168.2.2:6000, 9),
                              (1, 192.168.2.2:6000, 10),
                              (1, 192.168.2.2:6000, 11)],
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
