====================
Troubleshooting tips
====================

Diagnose: Customer complains they receive a HTTP status 500 when trying to browse containers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This entry is prompted by a real customer issue and exclusively focused on how
that problem was identified.
There are many reasons why a http status of 500 could be returned. If
there are no obvious problems with the swift object store, then it may
be necessary to take a closer look at the users transactions.
After finding the users swift account, you can
search the swift proxy logs on each swift proxy server for
transactions from this user. The linux ``bzgrep`` command can be used to
search all the proxy log files on a node including the ``.bz2`` compressed
files. For example:

.. code::

   $ PDSH_SSH_ARGS_APPEND="-o StrictHostKeyChecking=no" pdsh -l <yourusername> -R ssh \
     -w <redacted>.68.[4-11,132-139 4-11,132-139],<redacted>.132.[4-11,132-139] \
     'sudo bzgrep -w AUTH_redacted-4962-4692-98fb-52ddda82a5af /var/log/swift/proxy.log*' |  dshbak -c
    .
    .
    ----------------
    <redacted>.132.6
    ----------------
    Feb 29 08:51:57 sw-aw2az2-proxy011 proxy-server <redacted>.16.132
    <redacted>.66.8 29/Feb/2012/08/51/57 GET /v1.0/AUTH_redacted-4962-4692-98fb-52ddda82a5af
    /%3Fformat%3Djson HTTP/1.0 404 - - <REDACTED>_4f4d50c5e4b064d88bd7ab82 - - -
    tx429fc3be354f434ab7f9c6c4206c1dc3 - 0.0130

This shows a ``GET`` operation on the users account.

.. note::

   The HTTP status returned is 404, Not found, rather than 500 as reported by the user.

Using the transaction ID, ``tx429fc3be354f434ab7f9c6c4206c1dc3`` you can
search the swift object servers log files for this transaction ID:

.. code::

   $ PDSH_SSH_ARGS_APPEND="-o StrictHostKeyChecking=no" pdsh -l <yourusername> -R ssh \
     -w <redacted>.72.[4-67|4-67],<redacted>.[4-67|4-67],<redacted>.[4-67|4-67],<redacted>.204.[4-131] \
     'sudo bzgrep tx429fc3be354f434ab7f9c6c4206c1dc3 /var/log/swift/server.log*' | dshbak -c
   .
   .
   ----------------
   <redacted>.72.16
   ----------------
   Feb 29 08:51:57 sw-aw2az1-object013 account-server <redacted>.132.6 - -

   [29/Feb/2012:08:51:57 +0000|] "GET /disk9/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   404 - "tx429fc3be354f434ab7f9c6c4206c1dc3" "-" "-"

   0.0016 ""
   ----------------
   <redacted>.31
   ----------------
   Feb 29 08:51:57 node-az2-object060 account-server <redacted>.132.6 - -
   [29/Feb/2012:08:51:57 +0000|] "GET /disk6/198875/AUTH_redacted-4962-
   4692-98fb-52ddda82a5af" 404 - "tx429fc3be354f434ab7f9c6c4206c1dc3" "-" "-" 0.0011 ""
   ----------------
   <redacted>.204.70
   ----------------

   Feb 29 08:51:57 sw-aw2az3-object0067 account-server <redacted>.132.6 - -
   [29/Feb/2012:08:51:57 +0000|] "GET /disk6/198875/AUTH_redacted-4962-
   4692-98fb-52ddda82a5af" 404 - "tx429fc3be354f434ab7f9c6c4206c1dc3" "-" "-" 0.0014 ""

.. note::

   The 3 GET operations to 3 different object servers that hold the 3
   replicas of this users account. Each ``GET`` returns a HTTP status of 404,
   Not found.

Next, use the ``swift-get-nodes`` command to determine exactly where the
user's account data is stored:

.. code::

   $ sudo swift-get-nodes /etc/swift/account.ring.gz AUTH_redacted-4962-4692-98fb-52ddda82a5af
   Account AUTH_redacted-4962-4692-98fb-52ddda82a5af
   Container None
   Object None

   Partition 198875
   Hash 1846d99185f8a0edaf65cfbf37439696

   Server:Port Device <redacted>.31:6202 disk6
   Server:Port Device <redacted>.204.70:6202 disk6
   Server:Port Device <redacted>.72.16:6202 disk9
   Server:Port Device <redacted>.204.64:6202 disk11 [Handoff]
   Server:Port Device <redacted>.26:6202 disk11 [Handoff]
   Server:Port Device <redacted>.72.27:6202 disk11 [Handoff]

   curl -I -XHEAD "`http://<redacted>.31:6202/disk6/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   <http://15.185.138.31:6202/disk6/198875/AUTH_db0050ad-4962-4692-98fb-52ddda82a5af>`_
   curl -I -XHEAD "`http://<redacted>.204.70:6202/disk6/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   <http://15.185.204.70:6202/disk6/198875/AUTH_db0050ad-4962-4692-98fb-52ddda82a5af>`_
   curl -I -XHEAD "`http://<redacted>.72.16:6202/disk9/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   <http://15.185.72.16:6202/disk9/198875/AUTH_db0050ad-4962-4692-98fb-52ddda82a5af>`_
   curl -I -XHEAD "`http://<redacted>.204.64:6202/disk11/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   <http://15.185.204.64:6202/disk11/198875/AUTH_db0050ad-4962-4692-98fb-52ddda82a5af>`_ # [Handoff]
   curl -I -XHEAD "`http://<redacted>.26:6202/disk11/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   <http://15.185.136.26:6202/disk11/198875/AUTH_db0050ad-4962-4692-98fb-52ddda82a5af>`_ # [Handoff]
   curl -I -XHEAD "`http://<redacted>.72.27:6202/disk11/198875/AUTH_redacted-4962-4692-98fb-52ddda82a5af"
   <http://15.185.72.27:6202/disk11/198875/AUTH_db0050ad-4962-4692-98fb-52ddda82a5af>`_ # [Handoff]

   ssh <redacted>.31 "ls -lah /srv/node/disk6/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/"
   ssh <redacted>.204.70 "ls -lah /srv/node/disk6/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/"
   ssh <redacted>.72.16 "ls -lah /srv/node/disk9/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/"
   ssh <redacted>.204.64 "ls -lah /srv/node/disk11/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/" # [Handoff]
   ssh <redacted>.26 "ls -lah /srv/node/disk11/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/" # [Handoff]
   ssh <redacted>.72.27 "ls -lah /srv/node/disk11/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/" # [Handoff]

Check each of the primary servers, <redacted>.31, <redacted>.204.70  and <redacted>.72.16, for
this users account. For example on <redacted>.72.16:

.. code::

   $ ls -lah /srv/node/disk9/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/
   total 1.0M
   drwxrwxrwx 2 swift swift 98 2012-02-23 14:49 .
   drwxrwxrwx 3 swift swift 45 2012-02-03 23:28 ..
   -rw------- 1 swift swift 15K 2012-02-23 14:49 1846d99185f8a0edaf65cfbf37439696.db
   -rw-rw-rw- 1 swift swift 0 2012-02-23 14:49 1846d99185f8a0edaf65cfbf37439696.db.pending

So this users account db, an sqlite db is present. Use sqlite to
checkout the account:

.. code::

   $ sudo cp /srv/node/disk9/accounts/198875/696/1846d99185f8a0edaf65cfbf37439696/1846d99185f8a0edaf65cfbf37439696.db /tmp
   $ sudo sqlite3 /tmp/1846d99185f8a0edaf65cfbf37439696.db
   sqlite> .mode line
   sqlite> select * from account_stat;
   account = AUTH_redacted-4962-4692-98fb-52ddda82a5af
   created_at = 1328311738.42190
   put_timestamp = 1330000873.61411
   delete_timestamp = 1330001026.00514
   container_count = 0
   object_count = 0
   bytes_used = 0
   hash = eb7e5d0ea3544d9def940b19114e8b43
   id = 2de8c8a8-cef9-4a94-a421-2f845802fe90
   status = DELETED
   status_changed_at = 1330001026.00514
   metadata =

.. note:

   The status is ``DELETED``. So this account was deleted. This explains
   why the GET operations are returning 404, not found. Check the account
   delete date/time:

   .. code::

      $ python

      >>> import time
      >>> time.ctime(1330001026.00514)
      'Thu Feb 23 12:43:46 2012'

Next try and find the ``DELETE`` operation for this account in the proxy
server logs:

.. code::

   $ PDSH_SSH_ARGS_APPEND="-o StrictHostKeyChecking=no" pdsh -l <yourusername> -R ssh \
     -w <redacted>.68.[4-11,132-139 4-11,132-139],<redacted>.132.[4-11,132-139|4-11,132-139] \
     'sudo bzgrep AUTH_redacted-4962-4692-98fb-52ddda82a5af /var/log/swift/proxy.log* \
     | grep -w DELETE | awk "{print $3,$10,$12}"' |- dshbak -c
   .
   .
   Feb 23 12:43:46 sw-aw2az2-proxy001 proxy-server <redacted> <redacted>.66.7 23/Feb/2012/12/43/46 DELETE /v1.0/AUTH_redacted-4962-4692-98fb-
   52ddda82a5af/ HTTP/1.0 204 - Apache-HttpClient/4.1.2%20%28java%201.5%29 <REDACTED>_4f458ee4e4b02a869c3aad02 - - -
   tx4471188b0b87406899973d297c55ab53 - 0.0086

From this you can see the operation that resulted in the account being deleted.

Procedure: Deleting objects
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Simple case - deleting small number of objects and containers
-------------------------------------------------------------

.. note::

   ``swift-direct`` is specific to the Hewlett Packard Enterprise Helion Public Cloud.
   Use ``swiftly`` as an alternative.

.. note::

   Object and container names are in UTF8. Swift direct accepts UTF8
   directly, not URL-encoded UTF8 (the REST API expects UTF8 and then
   URL-encoded). In practice cut and paste of foreign language strings to
   a terminal window will produce the right result.

   Hint: Use the ``head`` command before any destructive commands.

To delete a small number of objects, log into any proxy node and proceed
as follows:

Examine the object in question:

.. code::

   $ sudo -u swift /opt/hp/swift/bin/swift-direct head 132345678912345 container_name obj_name

See if ``X-Object-Manifest`` or ``X-Static-Large-Object`` is set,
then this is the manifest object and segment objects may be in another
container.

If the ``X-Object-Manifest`` attribute is set, you need to find the
name of the objects this means it is a DLO. For example,
if ``X-Object-Manifest`` is ``container2/seg-blah``, list the contents
of the container container2 as follows:

.. code::

   $ sudo -u swift /opt/hp/swift/bin/swift-direct show 132345678912345 container2

Pick out the objects whose names start with ``seg-blah``.
Delete the segment objects as follows:

.. code::

   $ sudo -u swift /opt/hp/swift/bin/swift-direct delete 132345678912345 container2 seg-blah01
   $ sudo -u swift /opt/hp/swift/bin/swift-direct delete 132345678912345 container2 seg-blah02
   etc

If ``X-Static-Large-Object`` is set, you need to read the contents. Do this by:

-  Using swift-get-nodes to get the details of the object's location.
-  Change the ``-X HEAD`` to ``-X GET`` and run ``curl`` against one copy.
-  This lists a JSON body listing containers and object names
-  Delete the objects as described above for DLO segments

Once the segments are deleted, you can delete the object using
``swift-direct`` as described above.

Finally, use ``swift-direct`` to delete the container.

Procedure: Decommissioning swift nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Should Swift nodes need to be decommissioned (e.g.,, where they are being
re-purposed), it is very important to follow the following steps.

#. In the case of object servers, follow the procedure for removing
   the node from the rings.
#. In the case of swift proxy servers, have the network team remove
   the node from the load balancers.
#. Open a network ticket to have the node removed from network
   firewalls.
#. Make sure that you remove the ``/etc/swift`` directory and everything in it.
