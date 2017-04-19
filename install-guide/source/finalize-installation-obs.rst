.. _finalize-obs:

Finalize installation for openSUSE and SUSE Linux Enterprise
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Default configuration files vary by distribution. You might need
   to add these sections and options rather than modifying existing
   sections and options. Also, an ellipsis (``...``) in the configuration
   snippets indicates potential default configuration options that you
   should retain.

This section applies to openSUSE Leap 42.2 and SUSE Linux Enterprise Server
12 SP2.

#. Edit the ``/etc/swift/swift.conf`` file and complete the following
   actions:

   * In the ``[swift-hash]`` section, configure the hash path prefix and
     suffix for your environment.

     .. code-block:: none

        [swift-hash]
        ...
        swift_hash_path_suffix = HASH_PATH_SUFFIX
        swift_hash_path_prefix = HASH_PATH_PREFIX

     Replace HASH_PATH_PREFIX and HASH_PATH_SUFFIX with unique values.

     .. warning::

        Keep these values secret and do not change or lose them.

   * In the ``[storage-policy:0]`` section, configure the default
     storage policy:

     .. code-block:: none

        [storage-policy:0]
        ...
        name = Policy-0
        default = yes

#. Copy the ``swift.conf`` file to the ``/etc/swift`` directory on
   each storage node and any additional nodes running the proxy service.

3. On all nodes, ensure proper ownership of the configuration directory:

   .. code-block:: console

      # chown -R root:swift /etc/swift

4. On the controller node and any other nodes running the proxy service,
   start the Object Storage proxy service including its dependencies and
   configure them to start when the system boots:

   .. code-block:: console

      # systemctl enable openstack-swift-proxy.service memcached.service
      # systemctl start openstack-swift-proxy.service memcached.service

5. On the storage nodes, start the Object Storage services and configure
   them to start when the system boots:

   .. code-block:: console

      # systemctl enable openstack-swift-account.service openstack-swift-account-auditor.service \
        openstack-swift-account-reaper.service openstack-swift-account-replicator.service
      # systemctl start openstack-swift-account.service openstack-swift-account-auditor.service \
        openstack-swift-account-reaper.service openstack-swift-account-replicator.service
      # systemctl enable openstack-swift-container.service openstack-swift-container-auditor.service \
        openstack-swift-container-replicator.service openstack-swift-container-updater.service
      # systemctl start openstack-swift-container.service openstack-swift-container-auditor.service \
        openstack-swift-container-replicator.service openstack-swift-container-updater.service
      # systemctl enable openstack-swift-object.service openstack-swift-object-auditor.service \
        openstack-swift-object-replicator.service openstack-swift-object-updater.service
      # systemctl start openstack-swift-object.service openstack-swift-object-auditor.service \
        openstack-swift-object-replicator.service openstack-swift-object-updater.service
