==================
The Account Reaper
==================

The Account Reaper removes data from deleted accounts in the background.

An account is marked for deletion by a reseller issuing a DELETE request on the
account's storage URL. This simply puts the value DELETED into the status
column of the account_stat table in the account database (and replicas),
indicating the data for the account should be deleted later.

There is normally no set retention time and no undelete; it is assumed the
reseller will implement such features and only call DELETE on the account once
it is truly desired the account's data be removed. However, in order to protect
the Swift cluster accounts from an improper or mistaken delete request, you can
set a delay_reaping value in the [account-reaper] section of the
account-server.conf to delay the actual deletion of data. At this time, there
is no utility to undelete an account; one would have to update the account
database replicas directly, setting the status column to an empty string and
updating the put_timestamp to be greater than the delete_timestamp. (On the
TODO list is writing a utility to perform this task, preferably through a REST
call.)

The account reaper runs on each account server and scans the server
occasionally for account databases marked for deletion. It will only trigger on
accounts that server is the primary node for, so that multiple account servers
aren't all trying to do the same work at the same time. Using multiple servers
to delete one account might improve deletion speed, but requires coordination
so they aren't duplicating effort. Speed really isn't as much of a concern with
data deletion and large accounts aren't deleted that often.

The deletion process for an account itself is pretty straightforward. For each
container in the account, each object is deleted and then the container is
deleted. Any deletion requests that fail won't stop the overall process, but
will cause the overall process to fail eventually (for example, if an object
delete times out, the container won't be able to be deleted later and therefore
the account won't be deleted either). The overall process continues even on a
failure so that it doesn't get hung up reclaiming cluster space because of one
troublesome spot. The account reaper will keep trying to delete an account
until it eventually becomes empty, at which point the database reclaim process
within the db_replicator will eventually remove the database files.

Sometimes a persistent error state can prevent some object or container
from being deleted. If this happens, you will see a message such as "Account
<name> has not been reaped since <date>" in the log. You can control when
this is logged with the reap_warn_after value in the [account-reaper] section
of the account-server.conf file. By default this is 30 days.

-------
History
-------

At first, a simple approach of deleting an account through completely external
calls was considered as it required no changes to the system. All data would
simply be deleted in the same way the actual user would, through the public
REST API. However, the downside was that it would use proxy resources and log
everything when it didn't really need to. Also, it would likely need a
dedicated server or two, just for issuing the delete requests.

A completely bottom-up approach was also considered, where the object and
container servers would occasionally scan the data they held and check if the
account was deleted, removing the data if so. The upside was the speed of
reclamation with no impact on the proxies or logging, but the downside was that
nearly 100% of the scanning would result in no action creating a lot of I/O
load for no reason.

A more container server centric approach was also considered, where the account
server would mark all the containers for deletion and the container servers
would delete the objects in each container and then themselves. This has the
benefit of still speedy reclamation for accounts with a lot of containers, but
has the downside of a pretty big load spike. The process could be slowed down
to alleviate the load spike possibility, but then the benefit of speedy
reclamation is lost and what's left is just a more complex process. Also,
scanning all the containers for those marked for deletion when the majority
wouldn't be seemed wasteful. The db_replicator could do this work while
performing its replication scan, but it would have to spawn and track deletion
processes which seemed needlessly complex.

In the end, an account server centric approach seemed best, as described above.
