Create and distribute initial rings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Before starting the Object Storage services, you must create the initial
account, container, and object rings. The ring builder creates configuration
files that each node uses to determine and deploy the storage architecture.
For simplicity, this guide uses one region and two zones with 2^10 (1024)
maximum partitions, 3 replicas of each object, and 1 hour minimum time between
moving a partition more than once. For Object Storage, a partition indicates a
directory on a storage device rather than a conventional partition table.
For more information, see the
`Deployment Guide <https://docs.openstack.org/swift/latest/deployment_guide.html>`__.

.. note::
   Perform these steps on the controller node.

Create account ring
-------------------

The account server uses the account ring to maintain lists of containers.

#. Change to the ``/etc/swift`` directory.

#. Create the base ``account.builder`` file:

   .. code-block:: console

      # swift-ring-builder account.builder create 10 3 1

   .. note::

      This command provides no output.

#. Add each storage node to the ring:

   .. code-block:: console

      # swift-ring-builder account.builder \
        add --region 1 --zone 1 --ip STORAGE_NODE_MANAGEMENT_INTERFACE_IP_ADDRESS --port 6202 \
        --device DEVICE_NAME --weight DEVICE_WEIGHT

   Replace ``STORAGE_NODE_MANAGEMENT_INTERFACE_IP_ADDRESS`` with the IP address
   of the management network on the storage node. Replace ``DEVICE_NAME`` with a
   storage device name on the same storage node. For example, using the first
   storage node in :ref:`storage` with the ``/dev/sdb`` storage
   device and weight of 100:

   .. code-block:: console

      # swift-ring-builder account.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6202 --device sdb --weight 100

   Repeat this command for each storage device on each storage node. In the
   example architecture, use the command in four variations:

   .. code-block:: console

      # swift-ring-builder account.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6202 --device sdb --weight 100
      Device d0r1z1-10.0.0.51:6202R10.0.0.51:6202/sdb_"" with 100.0 weight got id 0
      # swift-ring-builder account.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6202 --device sdc --weight 100
      Device d1r1z2-10.0.0.51:6202R10.0.0.51:6202/sdc_"" with 100.0 weight got id 1
      # swift-ring-builder account.builder add \
        --region 1 --zone 2 --ip 10.0.0.52 --port 6202 --device sdb --weight 100
      Device d2r1z3-10.0.0.52:6202R10.0.0.52:6202/sdb_"" with 100.0 weight got id 2
      # swift-ring-builder account.builder add \
        --region 1 --zone 2 --ip 10.0.0.52 --port 6202 --device sdc --weight 100
      Device d3r1z4-10.0.0.52:6202R10.0.0.52:6202/sdc_"" with 100.0 weight got id 3

#. Verify the ring contents:

   .. code-block:: console

      # swift-ring-builder account.builder
      account.builder, build version 4
      1024 partitions, 3.000000 replicas, 1 regions, 2 zones, 4 devices, 100.00 balance, 0.00 dispersion
      The minimum number of hours before a partition can be reassigned is 1
      The overload factor is 0.00% (0.000000)
      Devices:    id  region  zone      ip address  port  replication ip  replication port      name weight partitions balance meta
                   0       1     1       10.0.0.51  6202       10.0.0.51              6202      sdb  100.00          0 -100.00
                   1       1     1       10.0.0.51  6202       10.0.0.51              6202      sdc  100.00          0 -100.00
                   2       1     2       10.0.0.52  6202       10.0.0.52              6202      sdb  100.00          0 -100.00
                   3       1     2       10.0.0.52  6202       10.0.0.52              6202      sdc  100.00          0 -100.00

#. Rebalance the ring:

   .. code-block:: console

      # swift-ring-builder account.builder rebalance
      Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00

Create container ring
---------------------

The container server uses the container ring to maintain lists of objects.
However, it does not track object locations.

#. Change to the ``/etc/swift`` directory.

#. Create the base ``container.builder`` file:

   .. code-block:: console

      # swift-ring-builder container.builder create 10 3 1

   .. note::

      This command provides no output.

#. Add each storage node to the ring:

   .. code-block:: console

      # swift-ring-builder container.builder \
        add --region 1 --zone 1 --ip STORAGE_NODE_MANAGEMENT_INTERFACE_IP_ADDRESS --port 6201 \
        --device DEVICE_NAME --weight DEVICE_WEIGHT

   Replace ``STORAGE_NODE_MANAGEMENT_INTERFACE_IP_ADDRESS`` with the IP address
   of the management network on the storage node. Replace ``DEVICE_NAME`` with a
   storage device name on the same storage node. For example, using the first
   storage node in :ref:`storage` with the ``/dev/sdb``
   storage device and weight of 100:

   .. code-block:: console

      # swift-ring-builder container.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6201 --device sdb --weight 100

   Repeat this command for each storage device on each storage node. In the
   example architecture, use the command in four variations:

   .. code-block:: console

      # swift-ring-builder container.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6201 --device sdb --weight 100
      Device d0r1z1-10.0.0.51:6201R10.0.0.51:6201/sdb_"" with 100.0 weight got id 0
      # swift-ring-builder container.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6201 --device sdc --weight 100
      Device d1r1z2-10.0.0.51:6201R10.0.0.51:6201/sdc_"" with 100.0 weight got id 1
      # swift-ring-builder container.builder add \
        --region 1 --zone 2 --ip 10.0.0.52 --port 6201 --device sdb --weight 100
      Device d2r1z3-10.0.0.52:6201R10.0.0.52:6201/sdb_"" with 100.0 weight got id 2
      # swift-ring-builder container.builder add \
        --region 1 --zone 2 --ip 10.0.0.52 --port 6201 --device sdc --weight 100
      Device d3r1z4-10.0.0.52:6201R10.0.0.52:6201/sdc_"" with 100.0 weight got id 3

#. Verify the ring contents:

   .. code-block:: console

      # swift-ring-builder container.builder
      container.builder, build version 4
      1024 partitions, 3.000000 replicas, 1 regions, 2 zones, 4 devices, 100.00 balance, 0.00 dispersion
      The minimum number of hours before a partition can be reassigned is 1
      The overload factor is 0.00% (0.000000)
      Devices:    id  region  zone      ip address  port  replication ip  replication port      name weight partitions balance meta
                   0       1     1       10.0.0.51  6201       10.0.0.51              6201      sdb  100.00          0 -100.00
                   1       1     1       10.0.0.51  6201       10.0.0.51              6201      sdc  100.00          0 -100.00
                   2       1     2       10.0.0.52  6201       10.0.0.52              6201      sdb  100.00          0 -100.00
                   3       1     2       10.0.0.52  6201       10.0.0.52              6201      sdc  100.00          0 -100.00

#. Rebalance the ring:

   .. code-block:: console

      # swift-ring-builder container.builder rebalance
      Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00

Create object ring
------------------

The object server uses the object ring to maintain lists of object locations
on local devices.

#. Change to the ``/etc/swift`` directory.

#. Create the base ``object.builder`` file:

   .. code-block:: console

      # swift-ring-builder object.builder create 10 3 1

   .. note::

      This command provides no output.

#. Add each storage node to the ring:

   .. code-block:: console

      # swift-ring-builder object.builder \
        add --region 1 --zone 1 --ip STORAGE_NODE_MANAGEMENT_INTERFACE_IP_ADDRESS --port 6200 \
        --device DEVICE_NAME --weight DEVICE_WEIGHT

   Replace ``STORAGE_NODE_MANAGEMENT_INTERFACE_IP_ADDRESS`` with the IP address
   of the management network on the storage node. Replace ``DEVICE_NAME`` with
   a storage device name on the same storage node. For example, using the first
   storage node in :ref:`storage` with the ``/dev/sdb`` storage
   device and weight of 100:

   .. code-block:: console

      # swift-ring-builder object.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6200 --device sdb --weight 100

   Repeat this command for each storage device on each storage node. In the
   example architecture, use the command in four variations:

   .. code-block:: console

      # swift-ring-builder object.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6200 --device sdb --weight 100
      Device d0r1z1-10.0.0.51:6200R10.0.0.51:6200/sdb_"" with 100.0 weight got id 0
      # swift-ring-builder object.builder add \
        --region 1 --zone 1 --ip 10.0.0.51 --port 6200 --device sdc --weight 100
      Device d1r1z2-10.0.0.51:6200R10.0.0.51:6200/sdc_"" with 100.0 weight got id 1
      # swift-ring-builder object.builder add \
        --region 1 --zone 2 --ip 10.0.0.52 --port 6200 --device sdb --weight 100
      Device d2r1z3-10.0.0.52:6200R10.0.0.52:6200/sdb_"" with 100.0 weight got id 2
      # swift-ring-builder object.builder add \
        --region 1 --zone 2 --ip 10.0.0.52 --port 6200 --device sdc --weight 100
      Device d3r1z4-10.0.0.52:6200R10.0.0.52:6200/sdc_"" with 100.0 weight got id 3

#. Verify the ring contents:

   .. code-block:: console

      # swift-ring-builder object.builder
      object.builder, build version 4
      1024 partitions, 3.000000 replicas, 1 regions, 2 zones, 4 devices, 100.00 balance, 0.00 dispersion
      The minimum number of hours before a partition can be reassigned is 1
      The overload factor is 0.00% (0.000000)
      Devices:    id  region  zone      ip address  port  replication ip  replication port      name weight partitions balance meta
                   0       1     1       10.0.0.51  6200       10.0.0.51              6200      sdb  100.00          0 -100.00
                   1       1     1       10.0.0.51  6200       10.0.0.51              6200      sdc  100.00          0 -100.00
                   2       1     2       10.0.0.52  6200       10.0.0.52              6200      sdb  100.00          0 -100.00
                   3       1     2       10.0.0.52  6200       10.0.0.52              6200      sdc  100.00          0 -100.00

#. Rebalance the ring:

   .. code-block:: console

      # swift-ring-builder object.builder rebalance
      Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00

Distribute ring configuration files
-----------------------------------

* Copy the ``account.ring.gz``, ``container.ring.gz``, and
  ``object.ring.gz`` files to the ``/etc/swift`` directory
  on each storage node and any additional nodes running the
  proxy service.
