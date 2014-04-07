=========================
SAIO and Storage Policies
=========================

Depending on when you downloaded your SAIO environment, it may already
be prepared with two storage policies that enable some basic functional
tests.  In the event that you are adding a storage policy to an existing
installation, however, the following section will walk you through the
steps for setting up Storage Policies.  Enabling Storage Policies is very
easy regardless of whether you are working with an existing installation
or starting a brand new one.  Note that for an existing installation,
without any of these modifications, the pre-existing object ring will be
automatically used for both existing and newly created containers.

Now we will create two policies - the first one will be a standard triple
replication policy that we will also explicitly set as the default and
the second will be setup for reduced replication using a factor of 2x.
We will call the first one 'gold' and the second one 'silver'.  In this
example both policies map to the same devices because it's also
important for the for this sample implementation to be simple and easy
to understand and adding a bunch of new devices isn't really required
to implement a usable set of policies.

1. Edit your ``/etc/swift/swift/conf`` file::

        [swift-hash]
        # random unique strings that can never change (DO NOT LOSE)
        swift_hash_path_prefix = changeme

        [storage-policy:0]
        name = gold
        default = yes

        [storage-policy:1]
        name = silver

  See :doc:`overview_policies` for detailed information on ``swift.conf`` policy
  options.

2. Create the ring file for the 'silver' policy and put it in ``/etc/swift``::

        swift-ring-builder object-1.builder create 10 2 1
        swift-ring-builder object-1.builder add r1z1-127.0.0.1:6010/sdb1 1
        swift-ring-builder object-1.builder add r1z2-127.0.0.1:6020/sdb2 1
        swift-ring-builder object-1.builder add r1z3-127.0.0.1:6030/sdb3 1
        swift-ring-builder object-1.builder add r1z4-127.0.0.1:6040/sdb4 1
        swift-ring-builder object-1.builder rebalance

  Note that the reduced replication of the silver policy is only a function
  of the replication parameter in the ``swift-ring-builder create`` command
  and is not specified  in ``/etc/swift/swift.conf``.

------------------
Using Policies
------------------

Setting up Storage Policies was very simple, and using them is even simpler.
In this section we will create a few containers with different policies and
then run some commands to verify that things land where we expect.

Now that you've updated your ``/etc/swift/swift.conf`` and created your second
object ring per the steps above, lets create a few containers and objects and
see how policies work.

1. We will be using the list_endpoints middleware to confirm object locations,
   so enable that now in your ``proxy-server.conf`` file by adding it to the pipeline
   and including the filter section as shown below::

        pipeline = catch_errors gatekeeper healthcheck proxy-logging cache bulk
        slo dlo ratelimit crossdomain list-endpoints tempurl tempauth staticweb
        container-quotas account-quotas proxy-logging proxy-server

        [filter:list-endpoints]
        use = egg:swift#list_endpoints

2. Create a container without specifying a policy, it will use the default,
   'gold' and then put a test object in it (create the file ``file0.txt`` with your
   favorite editor with some content)::

        curl -v -X PUT -H 'X-Auth-Token: AUTH_token' \
            http://127.0.0.1:8080/v1/AUTH_test/myCont0
        curl -X PUT -v -T file0.txt -H 'X-Auth-Token: AUTH_token' \
            http://127.0.0.1:8080/v1/AUTH_test/myCont0/

3. Now confirm placement of the object with the :ref:`list_endpoints` middleware::

        curl -X GET -v -H 'X-Auth-Token: AUTH_toeken' \
            http://127.0.0.1:8080/endpoints/v1/AUTH_test/myCont0/file0.txt

  You should see this: (note placement on expected devices)::

        ["http://127.0.0.1:6030/sdb3/761/v1/AUTH_test/myCont0/file0.txt",
        "http://127.0.0.1:6010/sdb1/761/v1/AUTH_test/myCont0/file0.txt",
        "http://127.0.0.1:6020/sdb2/761/v1/AUTH_test/myCont0/file0.txt"]

4. Create a container using policy 'silver' and put a different file in it::

        curl -v -X PUT -H 'X-Auth-Token: AUTH_token' -H "X-Storage-Policy: silver" \
            http://127.0.0.1:8080/v1/AUTH_test/myCont1
        curl -X PUT -v -T file1.txt -H 'X-Auth-Token: AUTH_token' \
            http://127.0.0.1:8080/v1/AUTH_test/myCont1/

5. Confirm placement of the object for policy 'silver'::

         curl -X GET -v -H 'X-Auth-Token: AUTH_token' \
            http://127.0.0.1:8080/endpoints/v1/AUTH_test/myCont1/file1.txt

  You should see this: (note placement on expected devices)::

        ["http://127.0.0.1:6010/sdb1/32/v1/AUTH_test/myCont1/file1.txt",
         "http://127.0.0.1:6040/sdb4/32/v1/AUTH_test/myCont1/file1.txt"]

6. Confirm account information with HEAD, make sure that your container-updater
   service is running and has executed once since you performed the PUTS or the
   account database won't be updated yet::

        curl -i -X HEAD -H 'X-Auth-Token: AUTH_token' \
        http://127.0.0.1:8080/v1/AUTH_test

  You should see something like this (note that total and per policy stats
  object sizes will vary)::

        HTTP/1.1 204 No Content
        Content-Length: 0
        X-Account-Object-Count: 2
        X-Account-Bytes-Used: 174
        X-Account-Container-Count: 2
        X-Account-Storage-Policy-Gold-Object-Count: 1
        X-Account-Storage-Policy-Gold-Bytes-Used: 84
        X-Account-Storage-Policy-Silver-Object-Count: 1
        X-Account-Storage-Policy-Silver-Bytes-Used: 90
        X-Timestamp: 1397230339.71525
        Content-Type: text/plain; charset=utf-8
        Accept-Ranges: bytes
        X-Trans-Id: tx96e7496b19bb44abb55a3-0053482c75
        Date: Fri, 11 Apr 2014 17:55:01 GMT
