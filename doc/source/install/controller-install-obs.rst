.. _controller-obs:

Install and configure the controller node for openSUSE and SUSE Linux Enterprise
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This section describes how to install and configure the proxy service that
handles requests for the account, container, and object services operating
on the storage nodes. For simplicity, this guide installs and configures
the proxy service on the controller node. However, you can run the proxy
service on any node with network connectivity to the storage nodes.
Additionally, you can install and configure the proxy service on multiple
nodes to increase performance and redundancy. For more information, see the
`Deployment Guide <https://docs.openstack.org/swift/latest/deployment_guide.html>`__.

This section applies to openSUSE Leap 42.2 and SUSE Linux Enterprise Server
12 SP2.

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

      # zypper install openstack-swift-proxy python-swiftclient \
        python-keystoneclient python-keystonemiddleware \
        python-xml memcached

   .. note::

      Complete OpenStack environments already include some of these
      packages.

   2. .. include:: controller-include.txt

