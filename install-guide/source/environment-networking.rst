.. _networking:

Configure networking
~~~~~~~~~~~~~~~~~~~~

Before you start deploying the Object Storage service in your OpenStack
environment, configure networking for two additional storage nodes.

First node
----------

Configure network interfaces
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Configure the management interface:

  * IP address: ``10.0.0.51``

  * Network mask: ``255.255.255.0`` (or ``/24``)

  * Default gateway: ``10.0.0.1``

Configure name resolution
^^^^^^^^^^^^^^^^^^^^^^^^^

#. Set the hostname of the node to ``object1``.

#. .. include:: edit_hosts_file.txt

#. Reboot the system to activate the changes.

Second node
-----------

Configure network interfaces
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* Configure the management interface:

  * IP address: ``10.0.0.52``

  * Network mask: ``255.255.255.0`` (or ``/24``)

  * Default gateway: ``10.0.0.1``

Configure name resolution
^^^^^^^^^^^^^^^^^^^^^^^^^

#. Set the hostname of the node to ``object2``.

#. .. include:: edit_hosts_file.txt

#. Reboot the system to activate the changes.

.. warning::

   Some distributions add an extraneous entry in the ``/etc/hosts``
   file that resolves the actual hostname to another loopback IP
   address such as ``127.0.1.1``. You must comment out or remove this
   entry to prevent name resolution problems. **Do not remove the
   127.0.0.1 entry.**

.. note::

   To reduce complexity of this guide, we add host entries for optional
   services regardless of whether you choose to deploy them.
