NOTE:  EC related docs are WIP and may/may not reflect the current state
of the codebase on the feature/ec branch.  They will continue to be a WIP
until we get closer and closer to code complete.

====================
Erasure Code Support
====================

-------------------------------
History and Theory of Operation
-------------------------------

There's a lot of good material out there on Erasure Code (EC) theory, this short
introduction is just meant to provide some basic context to help the reader
better understand the implementation in Swift.

Erasure Coding for storage applications grew out of Coding Theory as far back as
the 1960s with the Reed-Solomon codes.  These codes have been used for years in
applications ranging from CDs to DVDs to general communications and, yes, even in
the space program starting with Voyager! The basic idea is that some amount of data
is broken up into smaller pieces called fragments and coded in such a way that it
can be transmitted with the ability to tolerate the loss of some number of the
coded fragments.  That's where the word "erasure" comes in, if you transmit 14
fragments and only 13 are received then one of them is said to be "erased".
The word "erasure" provides an important distinction with EC; it isn't about
detecting errors, it's about dealing with failures.  Another important element of
EC is that the number of erasures that can be tolerated can be adjusted to meet
the needs of the application.

At a high level EC works by using a specific scheme to break up a single data buffer
into several smaller data buffers then, depending on the scheme, performing some encoding
operation on that data in order to generate additional information.  So you end up with more
data than you started with and that extra data is often called "parity".  Note that there are
many, many different encoding techniques that vary both in how they organize and manipulate
the data as well by what means they use to calculate parity.  For example, one scheme might
rely on `Galois Field Arithmetic <http://www.ssrc.ucsc.edu/Papers/plank-fast13.pdf>`_ while others may work with only XOR. The number of variations
and details about their differences are well beyond the scope of this introduction, but we'll
talk more about a few of them when we get into the implementation of EC in Swift.

--------------------------------
Overview of EC Support in Swift
--------------------------------

First and foremost, from an application perspective EC support is totally transparent. There
are no EC related external API; a container is simply created using a Storage Policy
defined to use EC and then interaction with the cluster is the same as any other durability
policy.

EC is implemented in Swift as a Storage Policy, see :doc:`overview_policies` for complete
details on Storage Policies.  Because support is implemented as a Storage Policy, all of
the storage devices associated with your cluster's EC capability can be isolated.  It is
entirely possible to share devices between storage policies, but for EC it may make more
sense to not only use separate devices but possibly even entire nodes dedicated for EC.

Which direction one chooses depends on why the EC policy is being deployed.  If, for
example, there is a production replication policy in place already and the goal is to add
a cold storage tier such that the existing nodes performing replication are impacted as
little as possible, adding a new set of nodes dedicated to EC might make the most sense
but also incurs the most cost.  On the other hand, if EC is being added as a capability
to provide additional durability for a specific set of applications and the existing
infrastructure is well suited for EC (sufficient number of nodes, zones for the EC scheme
that is chosen) then leveraging the existing infrastructure such that the EC ring shares
nodes with the replication ring makes the most sense.  These are some of the main
considerations:

* Layout of existing infrastructure
* Cost of adding a dedicated EC nodes (or just dedicated EC devices)
* Intended usage model(s)

The Swift code base doesn't include any of the algorithms necessary to perform the actual
encoding and decoding of data; that is left to an external library.  The Storage Policies
architecture is leveraged to allow EC on a per container basis and the object rings still
provide for the placement of EC data fragments.  Although there are several code paths that are
unique to an operation associated with an EC policy, an external dependency to an Erasure Code
library is what Swift counts on to perform the low level EC functions.  The use of an external
library allows for maximum flexibility as there are a significant number of options out there,
each with its owns pros and cons that can vary greatly from one use case to another.

---------------------------------------
PyECLib:  External Erasure Code Library
---------------------------------------

PyECLib is a Python Erasure Coding Library originally designed and written as part of the
effort to add EC support to the Swift project, however it is an independent project.  The
library provides a well-defined and simple Python interface and internally implements a
plug-in architecture allowing it to take advantage of many well-known C libraries such as:

* Jerasure at https://bitbucket.org/jimplank/jerasure
* GFComplete at https://bitbucket.org/jimplank/gf-complete
* Intel(R) ISA-L at https://01.org/intel%C2%AE-storage-acceleration-library-open-source-version
* Or write your own!

PyECLib itself therefore allows for not only choice, but further extensibility as well. PyECLib also
comes with a handy utility to help determine the best algorithm to use based on the equipment that
will be used (processors and server configurations may vary in performance per algorithm).  More on
this will be covered in the configuration section.  PyECLib is included as a Swift requirement.

For complete details see `PyECLib <https://bitbucket.org/kmgreen2/pyeclib>`_

------------------------------
Storing and Retrieving Objects
------------------------------

We will discuss the details of how PUT and GET work in the "Under the Hood" section later on.
The key point here is that all of the erasure code work goes on behind the scenes; this summary
is a high level information overview only.

The PUT flow looks like this:

#. The proxy server streams in an object and buffers up "a segment" of data (size is configurable)
#. The proxy server calls on PyECLib to encode the data into fragments
#. The proxy streams the encoded fragments out to the storage nodes based on ring locations
#. Repeat until the client is done sending data
#. The client is notified of completion when a quorum is met (configurable).

The GET flow looks like this:

#. The proxy server makes simultaneous requests to some number of participating nodes
#. As soon as the proxy has the fragments it needs, it calls on PyECLib to decode the data
#. The proxy streams the decoded data it has back to the client
#. Repeat until the proxy is done sending data back to the client

.. note::

    There are multiple options for how many and which nodes are used to retrieve fragments
    on a GET request with varying performance tradeoffs.  TODO:  once we characterize this
    we need to include what the exact options are and explain some of the details on
    the tradeoffs.

It may sound like, from this high level overview, that using EC is going to cause an
explosion in the number of actual files stored in each node's local file system.  Although
it is true that more files will be stored (because an object is broken into pieces), the
implementation works to minimize this where possible, more details are available in the
Under the Hood section.

-------------
Handoff Nodes
-------------

TODO

--------------
Reconstruction
--------------

For an EC policy, reconstruction is analogous to the process of replication for a replication
type policy -- essentially "the reconstructor" replaces "the replicator" for EC policy types.
The basic framework of reconstruction is very similar to that of replication with a
few notable exceptions:

* Because EC does not actually replicate partitions, it needs to operate at a finer granularity than what is provided with rsync, therefore EC leverages much of ssync behind the scenes (you do not need to configure things to use ssync).
* Once a pair of nodes has determined the need to replace a missing object fragment, instead of pushing over a copy like replication would do, the reconstructor has to read in enough surviving fragments from other nodes and perform a local reconstruction before it has the correct data to push to the other node.

.. note::

    EC work (encode and decode) takes place both on the proxy nodes, for PUT/GET operations, as
    well as on the storage nodes for reconstruction.  As with replication, reconstruction can
    be the result of rebalancing, bit-rot, drive failure or reverting data from a hand-off
    node back to its primary.

--------------------------
Performance Considerations
--------------------------

Big TODO here.

----------------------------
Using an Erasure Code Policy
----------------------------

To use an EC policy, the administrator simply needs to define an EC policy in `swift.conf`
and create/configure the associated object ring.  An example of how an EC policy can be
setup is shown below::

        [storage-policy:2]
        name = deepfreeze10-4
        type = erasure_coding
        ec_type = rs_vand
        ec_num_data_fragments = 10
        ec_num_parity_fragments = 4

Let's take a closer look at each configuration parameter:

* name: this is a standard storage policy parameter. See :doc:`overview_policies` for details.
* type: set this to 'erasure_coding' to indicate that this is an EC policy
* ec_type: set this value according to the available options in the selected PyECLib back-end. This specifies the EC scheme that is to be used.  For example the option shown here selects Vandermonde Reed-Solomon encoding while an option of 'flat_xor_3' would select Flat-XOR based HD combination codes.  See the `PyECLib <https://bitbucket.org/kmgreen2/pyeclib>`_ page for full details.
* ec_num_data_fragments:  the total number of fragments that will be comprised of data
* ec_num_parity_fragments:  the total number of fragments that will be comprised of parity

When PyECLib encodes an object, it will break it into N fragments however during configuration
what's important is how many of those are data and how many are parity.  So in the example above,
PyECLib will actually break an object in 14 different fragments, 10 of them will be made up of
actual object data and 4 of them will be made of parity data (calculations depending on ec_type).

When deciding which devices to use in the EC policy's object ring, be sure to carefully consider
the performance items mentioned earlier.  Once you've made you changes to `swift.conf` to
configure your EC policy, and created your object ring, your application is ready to start using EC
simply by creating a container with the specified name and interacting as usual.

Migrating Between Policies
--------------------------

A common usage of EC is to migrate less commonly accessed data from a more expensive but
lower latency policy such as replication.  When an application determines that it wants to
move data from a replication policy to an EC policy, it simply needs to move the data from
the EC container to a different container that was created with the target durability policy.

--------------
Under the Hood
--------------

Now that we've explained a little about EC support in Swift and how to configure/use it,
let's explore how EC fits in at the nuts-n-bolts level.

Terminology
-----------

The term 'fragment' has been used already to describe the output of the EC process (a series of
fragments) however we need to define some other key terms here before going any deeper.  Without
paying special attention to using the correct terms consistently, it is very easy to get confused
in a hurry!

* segment: not to be confused with SLO/DLO use of the work, in EC we call a segment a series of consecutive HTTP chunks buffered up before performing an EC operation.
* fragment: data and parity 'fragments' are generated when erasure coding transformation is applied to a segment.
* EC archive: A concatenation of EC fragments; to a storage node this looks like an object
* ec_k - number of EC data fragments (k is commonly used in the EC community for this purpose)
* ec_m - number of EC parity fragments (m is commonly used in the EC community for this purpose)
* chunk: HTTP chunks received over wire (term not used to describe any EC specific operation)

Middleware
----------

Middleware remains unchanged.  For most middleware (e.g., SLO/DLO) the fact that the proxy
is fragmenting incoming objects is transparent.  For list endpoints, however, it is a bit different.
A caller of list endpoints will get back the locations of all of the fragments.  The caller will be
unable to re-assemble the original object with this information, however the node locations may
still prove to be useful information for some applications.

On Disk Storage
---------------

EC archives are stored on disk in their respective objects-N directory based on their policy
index.  See :doc:`overview_policies` for details on per policy directory information.  There are
no special on disk storage impacts to EC policies.

Proxy Server
------------

TODO

Object Server
-------------

TODO

Metadata
--------

TODO

Database Updates
----------------

TODO

The Reconstructor
-----------------

TODO

The Auditor
-----------

Because the auditor already operates on a per storage policy basis, there are no specific
auditor changes associated with EC.  Each EC archive looks like, and is treated like, a
regular object from the perspective of the auditor.  Therefore, if the auditor finds bit-rot
in an EC archive, it simply quarantines it and the EC reconstructor will take care of the rest
just as the replicator does for replication policies.

PyECLib
-------

TODO
