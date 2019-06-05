.. _finalize-ubuntu-debian:

Finalize installation for Ubuntu and Debian
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   Default configuration files vary by distribution. You might need
   to add these sections and options rather than modifying existing
   sections and options. Also, an ellipsis (``...``) in the configuration
   snippets indicates potential default configuration options that you
   should retain.

This section applies to Ubuntu 14.04 (LTS) and Debian.

#. Obtain the ``/etc/swift/swift.conf`` file from the Object
   Storage source repository:

   .. code-block:: console

      # curl -o /etc/swift/swift.conf \
        https://opendev.org/openstack/swift/raw/branch/master/etc/swift.conf-sample

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

4. On all nodes, ensure proper ownership of the configuration directory:

   .. code-block:: console

      # chown -R root:swift /etc/swift

5. On the controller node and any other nodes running the proxy service,
   restart the Object Storage proxy service including its dependencies:

   .. code-block:: console

      # service memcached restart
      # service swift-proxy restart

6. On the storage nodes, start the Object Storage services:

   .. code-block:: console

      # swift-init all start

   .. note::

      The storage node runs many Object Storage services and the
      :command:`swift-init` command makes them easier to manage.
      You can ignore errors from services not running on the storage node.
