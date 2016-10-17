.. _verify:

Verify operation
~~~~~~~~~~~~~~~~

Verify operation of the Object Storage service.

.. note::

   Perform these steps on the controller node.

.. warning::

   If you are using Red Hat Enterprise Linux 7 or CentOS 7 and one or more of
   these steps do not work, check the ``/var/log/audit/audit.log`` file for
   SELinux messages indicating denial of actions for the ``swift`` processes.
   If present, change the security context of the ``/srv/node`` directory to
   the lowest security level (s0) for the ``swift_data_t`` type, ``object_r``
   role and the ``system_u`` user:

   .. code-block:: console

      # chcon -R system_u:object_r:swift_data_t:s0 /srv/node

#. Source the ``demo`` credentials:

   .. code-block:: console

      $ . demo-openrc

#. Show the service status:

   .. code-block:: console

      $ swift stat
                              Account: AUTH_ed0b60bf607743088218b0a533d5943f
                           Containers: 0
                              Objects: 0
                                Bytes: 0
          X-Account-Project-Domain-Id: default
                          X-Timestamp: 1444143887.71539
                           X-Trans-Id: tx1396aeaf17254e94beb34-0056143bde
               X-Openstack-Request-Id: tx1396aeaf17254e94beb34-0056143bde
                         Content-Type: text/plain; charset=utf-8
                        Accept-Ranges: bytes

#. Create ``container1`` container:

   .. code-block:: console

      $ openstack container create container1
      +---------------------------------------+------------+------------------------------------+
      | account                               | container  | x-trans-id                         |
      +---------------------------------------+------------+------------------------------------+
      | AUTH_ed0b60bf607743088218b0a533d5943f | container1 | tx8c4034dc306c44dd8cd68-0056f00a4a |
      +---------------------------------------+------------+------------------------------------+

#. Upload a test file to the ``container1`` container:

   .. code-block:: console

      $ openstack object create container1 FILE
      +--------+------------+----------------------------------+
      | object | container  | etag                             |
      +--------+------------+----------------------------------+
      | FILE   | container1 | ee1eca47dc88f4879d8a229cc70a07c6 |
      +--------+------------+----------------------------------+

   Replace ``FILE`` with the name of a local file to upload to the
   ``container1`` container.

#. List files in the ``container1`` container:

   .. code-block:: console

      $ openstack object list container1
      +------+
      | Name |
      +------+
      | FILE |
      +------+

#. Download a test file from the ``container1`` container:

   .. code-block:: console

      $ openstack object save container1 FILE

   Replace ``FILE`` with the name of the file uploaded to the
   ``container1`` container.

   .. note::

      This command provides no output.
