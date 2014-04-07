================
Storage Policies
================

Storage Policies allow for some level of segmenting the cluster for various
purposes through the creation of multiple object rings. Storage Policies are
not implemented as a separate code module but are an important concept in
understanding Swift architecture.

As described in :doc:`overview_ring`, Swift uses modified hashing rings to
determine where data should reside in the cluster. There is a separate ring
for account databases, container databases, and there is also one object
ring per storage policy.  Each object ring behaves exactly the same way
and is maintained in the same manner, but with policies, different devices
can belong to different rings with varrying levels of replication. By supporting
multiple object rings, Swift allows the application and/or deployer to
essentially segregate the object storage within a single cluster.  There are
many reasons why this might be desirable:

* Different levels of replication:  If a provider wants to offer, for example,
  2x replication and 3x replication but doesn't want to maintain 2 separate clusters,
  they would setup a 2x policy and a 3x policy and assign the nodes to their
  respective rings.

* Performance:  Just as SSDs can be used as the exclusive members of an account or
  database ring, an SSD-only object ring can be created as well and used to
  implement a low-latency/high performance policy.

* Collecting nodes into group:  A few examples here would be to assure data is
  always placed in a particular data center or geography, one could assign all devices
  belonging to that set of nodes to a single policy.  Another example would be to collect
  together a set of nodes that use a different Diskfile (e.g., Kinetic, Gluster) and use
  a policy to direct traffic just to those nodes.

NOTE: Today, choosing a different storage policy allows the use of different object rings,
but future policies (such as Erasure Coding) will also change some of the actual code
paths when processing a request.  Also note that Diskfile refers to backend object
storage plug-in architecture.

-----------------------
Containers and Policies
-----------------------
How are policies actually used by Swift?  Well, policies are implemented at the
container level.  There are many advantages to this approach, not the least of which
is how easy it makes life on applications that want to take advantage of them. Each
container has a new special immutable metadata element called the storage policy
index.  Note that internally, Swift relies on policy indexes and not policy names.
Policy names are there for human readability and translation is managed in the proxy.
When a container is created, one new optional header is supported to specify the
policy name.  If nothing is specified, the default policy is used (and if no default was
specified, Policy_0 is considered the default).  We will be covering the difference between
default and policy_0 in the next section.

Once a container has been assigned a policy, it can not be changed.  The implications
on data placement/movement for large datasets would make this a task best left for
applications to decide if/where/when. Therefore, if a container has an existing policy of,
for example 3x replication, and one wanted to migrate that data to a policy that specifies,
a different replication level, the application would create another container
specifying the other policy name and then simply move the data from one container
to the other.  Policies apply on a per container basis allowing for minimal application
awareness; once a container has been created with a specific policy, all objects stored
in it will be done so in accordance with that policy.

Containers have a many-to-one relationship with policies meaning that any number
of containers can share one policy.  There is no limit to how many containers can use
a specific policy.

The notion of associating a ring with a container introduces an interesting scenario:
What would happen if 2 containers of the same name were created with different
Storage Policies on either side of a network outage at the same time?  Furthermore,
what would happen if objects were placed in those containers, a whole bunch of them,
and then later the network outage was restored?  Well, without special care it would
be a big problem as an application could end up using the wrong ring to try and find
an object.  Luckily there is a solution for this problem, a daemon covered in more
detail later, works tirelessly to identify and rectify this potential scenario.

<TODO:  discuss reconciler>

-------------------------
Default versus 'Policy 0'
-------------------------
Storage Policies is a versatile feature intended to support both new and pre-existing
clusters with the same level of flexibility.  For that reason, we introduce the 'Policy 0'
concept which is not the same as the 'default' policy.  As you will see when we begin to
configure policies, each policy has both a name (human friendly, configurable) as well
as an index (or simply policy number). Swift reserves index 0 to map to the object ring
that's present in all installations (e.g., ``/etc/swift/object.ring.gz``).  You can name
this policy anything you like, and if no name is given it will report itself as Policy_0,
however you cannot change the index as there must always be a policy with index 0.

Another important concept is the default policy which can be any policy in the
cluster.  The default policy is the policy that is automatically chosen when a container
creation request is sent without a storage policy being specified. The difference from
Policy_0 is subtle but extremely important.  Policy_0 is what is used by Swift when
accessing pre-Storage Policy containers which won't have a policy - in this case we would
not use the default as it might not have the same policy as legacy containers.  When
no default is specified, Swift will always choose Policy_0 as the default.

In other words, default means 'create using this policy if nothing else is specified'
and Policy_0 means 'use the legacy policy if a container doesn't have one' which
really means use ``object.ring.gz`` for lookups.  We'll provide some examples of each of
these in the upcoming sections.

NOTE: With the Storage Policy based code, it's not possible to create a container that
doesn't have a policy.  If nothing is provided, Swift will still select the default
and assign it to the container.  For containers created before Storage Policies were
introduced, the legacy Policy_0 will be used.

---------------
On Disk Storage
---------------
Policies each have their own directories on the back end servers and are identified by
their storage policy indexes.  Organizing the back-end directory structures by policy
index helps keep track of things and also allows for sharing of disks between policies
which may or may not make sense depending on the needs of the provider.  More
on this later, but for now be aware that the following directory naming convention:

* ``/objects`` maps to objects associated with Policy_0
* ``/objects-N`` maps to storage policy index #N
* ``/async_pending`` maps to async pending update for Policy_0
* ``/async_pending-N`` maps to async pending update for storage policy index #N

Note that these directory names are actually owned by the specific Diskfile
Implementation, the names shown above are used by the default Diskfile.

--------------------
Configuring Policies
--------------------
Policies are configured in ``swift.conf`` and it is important that the deployer have a solid
understanding of the semantics for configuring policies.  Recall that a policy must have
a corresponding ring file, so configuring a policy is a two-step process.  First, edit
your ``/etc/swift/swift.conf`` file to add your new policy and, second, create the
corresponding policy object ring file.

See :doc:`policies_saio` for a step by step guide on adding a policy to the SAIO setup.

Note that each policy has a section starting with ``[storage-policy:N]`` where N is the
policy index.  There's no reason other than readability that these be sequential but there
are a number of rules enforced by Swift when parsing this file:

* If a policy with index 0 is not declared or no policies defined, Swift will create one
* The policy index must be a positive integer
* If no policy is declared as the default, the policy with index 0 is set as the default
* Policy indexes must be unique
* Policy names are required
* Policy names must be unique
* No more than 1 policy can be declared default
* Only supported types are allowed.  If no type is declared, the default of replication is
automatically selected.  The type field is used to differentiate durability policies.

The following is an example of a properly configured ''swift.conf'' file. See :doc:`policies_saio`
for full instructions on setting up an all-in-one with this example configuration.

        [swift-hash]
        # random unique strings that can never change (DO NOT LOSE)
        swift_hash_path_prefix = changeme
        swift_hash_path_suffix = changeme

        [storage-policy:0]
        name = gold
        default = yes

        [storage-policy:1]
        name = silver

There are some other considerations when managing policies:

* Policy names can be changed (but be sure that users are aware!)
* The default policy can be changed at any time
* You cannot change the index of a policy once it has been created
* Do not delete a policy, if one is no longer needed simply don't use it.
  It might be a good idea to name it something unique indicating that
  it is no longer valid, like '-deprecated'

There will be additional parameters for policies as new features are added
(e.g., Erasure Code), but for now only a section name/index and name
are required.  Once swift.conf is configured for a new policy, a new ring
must be created.  The ring tools are not policy name aware so it's critical
that the correct policy index be used when creating the new policy's ring
file.  Its created in the same manner as the legacy ring except that 'N' is
appended after the word ``object`` where N matches the policy index
used in swift.conf.  So, to create the ring for policy 1::

        swift-ring-builder object-1.builder create 10 3 1
        <and add devices, rebalance using the same naming convention>

NOTE:  The same drives can indeed be used for multiple policies and the
details of how that's managed on disk will be covered in a later section, it's
important to understand the implications of such a configuration before
setting one up.  Make sure it's really what you want to do, in many cases it
will be but in others maybe not.

--------------
Using Policies
--------------

Using policies is very simple, a policy is only specified when a container is
initially created, there are no other API changes.  Creating a container can
be done without any special policy information::

        curl -v -X PUT -H 'X-Auth-Token: AUTH_token' http://127.0.0.1:8080/v1/ \
            AUTH_test/myCont0

Which will result in a container created that is associated with the policy
name 'gold' assuming we're using the swift.conf example from the SAIO
setup.  It would use 'gold' because it was specified as the default.  Now,
when we put an object into this container, it will get placed on nodes that
are part of the ring we created for policy 'gold'.

If we wanted to explicitly state that we wanted policy 'gold' the command
would simply need to include a new header as shown below::

        curl -v -X PUT -H 'X-Auth-Token: AUTH_token' -H 'X-Storage-Policy: gold' \
            http://127.0.0.1:8080/v1/AUTH_test/myCont1

And that's it!  The application does not need to specify the policy name ever
again.  There are some illegal operations however:

* If an invalid (typo, non-existent) policy is specified: 400 Bad Request
* if you try to change the policy either via PUT or POST: 409 Conflict

If you'd like to see how the storage in the cluster is being used, simply HEAD
the account and you'll see not only the cumulative numbers, as before, but
per policy statistics as well.  In the example below there's 3 objects total
with two of them in policy 'gold' and one in policy 'silver'::

        curl -i -X HEAD -H 'X-Auth-Token: AUTH_token' \
            http://127.0.0.1:8080/v1/AUTH_test

and your results will include (some output removed for readability)::

        X-Account-Container-Count: 3
        X-Account-Object-Count: 3
        X-Account-Bytes-Used: 21
        X-Storage-Policy-Gold-Object-Count: 2
        X-Storage-Policy-Gold-Bytes-Used: 14
        X-Storage-Policy-Silver-Object-Count: 1
        X-Storage-Policy-Silver-Bytes-Used: 7

--------------
Under the Hood
--------------

Now that we've explained a little about what Policies are and how to
configure/use them, let's explore how Storage Policies fits in at the
nuts-n-bolts level.

Parsing and Configuring
-----------------------

A new module, :ref:`storage_policy`, is responsible for parsing the
``swift.conf`` file, validating the input, and creating a global collection of
configured policies via class :class:`.StoragePolicyCollection`.  This
collection is made up of policies of class :class:`.StoragePolicy`. The
collection class includes handy functions for getting to a policy either
by name or by index , getting info about the policies, etc.  There's
also one very important function, :meth:`~.StoragePolicyCollection.get_object_ring`.  Object rings
are now members of the :class:`.StoragePolicy` class and are actually not
instantiated until needed for the first time.  Any caller anywhere
in the code base that needs to access an object ring must use
the :data:`.POLICIES` global singleton to access the :meth:`~.StoragePolicyCollection.get_object_ring` function
and provide the policy index.  The global is instantiated when Swift
starts and provides a mechanism to patch policies for the test code.

Middleware
----------

Middleware can take advantage of policies through the :data:`.POLICIES` global
And by importing :func:`.get_container_info` to gain access to the policy
index associated with the container in question.  From the index it
can then use the :data:`.POLICIES` singleton to grab the right ring.  For example,
:ref:`list_endpoints` is policy aware using the means just described. Another
example is :ref:`recon` which will report the md5 sums for all object rings.

Proxy Server
------------

The :ref:`proxy-server` module's role in Storage Policies is essentially to make sure the
correct ring is used as its member element.  Before policies, the one object ring
would be instantiated when the :class:`.Application` class was instantiated and could
be overridden by test code via init parameter.  With policies, however, there is
no init parameter and the :class:`.Application` class instead depends on the :data:`.POLICIES`
global singleton to retrieve the ring which is instantiated the first time its
needed.  So, instead of a object ring member of the :class:`.Application` class, there is
an accessor function, :meth:`~.Application.get_object_ring`, that gets the ring from :data:`.POLICIES`.

In general, when any module running on the proxy requires an object ring, it
does so via first getting the policy index from the cached container info.  The
exception is during container creation where it uses the policy name from the
request header to look up policy index from the :data:`.POLICIES` global.  Once the
proxy has determined the policy index, it can use the :meth:`~.Application.get_object_ring` method
described earlier to gain access to the correct ring.  It then has the responsibility
of passing the index information, not the policy name, on to the back-end servers
via a the header ``X-Storage-Policy-Index``. Going the other way, the proxy also
strips the index out of headers that go back to clients and make sure they only
see the friendly policy names.

Object Server
-------------

It may not seem like the :ref:`object-server` would be too involved with Policies and
if you thought that it should be then you'd be correct.  However, because of
how back-end directory structures are setup for policies, as described earlier,
the object server modules do play a role.  When the object server gets a :class:`.DiskFile`,
it passes in the policy index and leaves the actual directory naming/structure
mechanisms to :class:`.DiskFile`.  By passing in the index, the instance of :class:`.DiskFile` being
used will assure that data is properly located in the tree based on its policy.

For the same reason, the :ref:`object-updater` also is policy aware; as previously
described, different policies use different async pending directories so the
updater needs to know how to scan them appropriately.

The :ref:`object-replicator` is policy aware in that, depending on the policy, it may have to
do drastically different things, or maybe not.  For example, the difference in
handling a replication job for 2x versus 3x is trivial however the difference in
handling replication between 3x and erasure code is most definitely not.  In
fact, the term 'replication' really isn't appropriate for some policies
like erasure code however the majority of the framework for collecting and
processing jobs remains the same.  Thus, those functions in the replicator are
leveraged for all policies and then there is policy specific code required for
each policy, added when the policy is defined if needed.

The new ssync functionality is policy aware for the same reason. Some of the
other modules may not obviously be affected, but the back-end directory
structure owned by :class:`.DiskFile` requires the policy index parameter.
Therefore ssync being policy aware really means passing the policy index along.

For :class:`.DiskFile` itself, being policy aware is all about managing the back-end
structure using the provided policy index.  In other words, callers who get
a :class:`.DiskFile` instance provide a policy index and :class:`.DiskFile`'s job is to keep data
separated via this index (however it chooses) such that policies can share
the same media/nodes if desired.  The included implementation of :class:`.DiskFile`
lays out the directory structure described earlier but that's owned within
:class:`.DiskFile`; external modules have no visibility into that detail.  A common
function is provided to map various directory names and/or strings
based on their policy index. For example :class:`.DiskFile` defines :func:`.get_data_dir`
which builds off of a generic :func:`.get_policy_string` to consistently build
policy aware strings for various usage.

Container Server
----------------

The :ref:`container-server` plays a very important role in Storage Policies, it is
responsible for handling the assignment of a policy to a container and the
prevention of bad things like changing policies or picking the wrong policy
to use when nothing is specified (recall earlier discussion on Policy_0 versus
default).

The :ref:`container-updater` is policy aware, however its job is very simple, to
pass the policy index along to the :ref:`account-server` via a request header.

The :ref:`container-backend` is responsible for both altering existing DB schema
as well as assuring new DBs are created with the storage policy index as a
new column in the ``container_stat`` table.  The policy index is stored here
for use in reporting information about the container as well as managing
split-brain scenario induced discrepancies between containers and their
storage policies.

The :ref:`container-sync-daemon` functionality only needs to be policy aware in that it
accesses the object rings.  Therefore, it needs to pull the policy index
out of the container information and use it to select the appropriate
object ring from the :data:`.POLICIES` global.

Account Server
--------------

The :ref:`account-server`'s role in Storage Policies is really limited to reporting.
When a HEAD request is made on an account (see example provided earlier),
the account server is provided with the storage policy index and builds
the ``object_count`` and ``byte_count`` information for the client on a per
policy basis.

The account servers can do this because of some policy specific DB schema
changes.  A policy specific table, ``policy_stat``, maintains information on
a per policy basis (one row per policy) in the same manner in which
the ``account_stat`` table does.  The ``account_stat`` table still serves the same
purpose and is not replaced by ``policy_stat``, it holds the total cluster
stats whereas ``policy_stat`` just has the break downs.  The backend is
also responsible for migrating pre-storage-policy containers by altering
the DB schema and populating the ``policy_stat`` table for Policy_0 with
current ``account_stat`` data at that point in time.

Upgrading and Confirming Functionality
--------------------------------------

Upgrading to a version of Swift that has Storage Policy support is not difficult,
in fact, the cluster administrator isn't required to make any special configuration
changes to get going.  Swift will automatically begin using the existing object
ring as both the default ring and the Policy_0 ring.  Adding the declaration of
policy 0 is totally optional and in its absence, the name given to the implicit
policy 0 will be 'Policy_0'.  Let's say for testing purposes that you wanted to take
an existing cluster that already has lots of data on it and upgrade to Swift with
Storage Policies. From there you want to go ahead and create a policy and test a
few things out.  All you need to do is is:

  #. Define your policies in ``/etc/swift/swift.conf``
  #. Create the corresponding object rings
  #. Create containers and objects and confirm their placement is as expected

For a specific example that takes you through these steps, please see
:doc:`policies_saio`

NOTE:  If you downgrade from a Storage Policy enabled version of Swift to an
older version that doesn't support policies, you will not be able to access
any data stored in policies other than the policy with index 0.
