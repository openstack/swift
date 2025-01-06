.. _controller-rdo:

Install and configure the controller node for Red Hat Enterprise Linux and CentOS
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section describes how to install and configure the proxy service that
handles requests for the account, container, and object services operating
on the storage nodes. For simplicity, this guide installs and configures
the proxy service on the controller node. However, you can run the proxy
service on any node with network connectivity to the storage nodes.
Additionally, you can install and configure the proxy service on multiple
nodes to increase performance and redundancy. For more information, see the
`Deployment Guide <https://docs.openstack.org/swift/latest/deployment_guide.html>`__.

This section applies to Red Hat Enterprise Linux 9 and CentOS stream9.

.. include:: controller-common_prerequisites.txt

Install and configure components
--------------------------------

.. note::

   Default configuration files vary by distribution. You might need
   to add these sections and options rather than modifying existing
   sections and options. Also, an ellipsis (``...``) in the configuration
   snippets indicates potential default configuration options that you
   should retain.

#. Install the packages:

   .. code-block:: console

      # dnf install openstack-swift-proxy python3-swiftclient \
        python3-keystoneclient python3-keystonemiddleware \
        memcached

   .. note::

      Complete OpenStack environments already include some of these
      packages.

   2. Obtain the proxy service configuration file from the Object Storage
      source repository:

      .. code-block:: console

         # curl -o /etc/swift/proxy-server.conf https://opendev.org/openstack/swift/raw/branch/master/etc/proxy-server.conf-sample

   3. .. include:: controller-include.txt
