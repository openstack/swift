==============
Erasure coding
==============

Erasure coding is a set of algorithms that allows the reconstruction of
missing data from a set of original data. In theory, erasure coding uses
less capacity with similar durability characteristics as replicas.
From an application perspective, erasure coding support is transparent.
Object Storage (swift) implements erasure coding as a Storage Policy.
See :doc:`/overview_policies` for more details.

There is no external API related to erasure coding. Create a container using a
Storage Policy; the interaction with the cluster is the same as any
other durability policy. Because support implements as a Storage Policy,
you can isolate all storage devices that associate with your cluster's
erasure coding capability. It is entirely possible to share devices between
storage policies, but for erasure coding it may make more sense to use
not only separate devices but possibly even entire nodes dedicated for erasure
coding.
