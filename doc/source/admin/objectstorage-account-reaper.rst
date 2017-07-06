==============
Account reaper
==============

The purpose of the account reaper is to remove data from the deleted accounts.

A reseller marks an account for deletion by issuing a ``DELETE`` request
on the account's storage URL. This action sets the ``status`` column of
the account_stat table in the account database and replicas to
``DELETED``, marking the account's data for deletion.

Typically, a specific retention time or undelete are not provided.
However, you can set a ``delay_reaping`` value in the
``[account-reaper]`` section of the ``account-server.conf`` file to
delay the actual deletion of data. At this time, to undelete you have to update
the account database replicas directly, set the status column to an
empty string and update the put_timestamp to be greater than the
delete_timestamp.

.. note::

   It is on the development to-do list to write a utility that performs
   this task, preferably through a REST call.

The account reaper runs on each account server and scans the server
occasionally for account databases marked for deletion. It only fires up
on the accounts for which the server is the primary node, so that
multiple account servers aren't trying to do it simultaneously. Using
multiple servers to delete one account might improve the deletion speed
but requires coordination to avoid duplication. Speed really is not a
big concern with data deletion, and large accounts aren't deleted often.

Deleting an account is simple. For each account container, all objects
are deleted and then the container is deleted. Deletion requests that
fail will not stop the overall process but will cause the overall
process to fail eventually (for example, if an object delete times out,
you will not be able to delete the container or the account). The
account reaper keeps trying to delete an account until it is empty, at
which point the database reclaim process within the db\_replicator will
remove the database files.

A persistent error state may prevent the deletion of an object or
container. If this happens, you will see a message in the log, for example:

.. code-block:: console

   Account <name> has not been reaped since <date>

You can control when this is logged with the ``reap_warn_after`` value in the
``[account-reaper]`` section of the ``account-server.conf`` file.
The default value is 30 days.
