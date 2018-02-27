============================
Object Storage Install Guide
============================

.. toctree::
   :maxdepth: 2

   get_started.rst
   environment-networking.rst
   controller-install.rst
   storage-install.rst
   initial-rings.rst
   finalize-installation.rst
   verify.rst
   next-steps.rst

The Object Storage services (swift) work together to provide
object storage and retrieval through a REST API.

This chapter assumes a working setup of OpenStack following the
`OpenStack Installation Tutorial <https://docs.openstack.org/latest/install/>`_.

Your environment must at least include the Identity service (keystone)
prior to deploying Object Storage.
