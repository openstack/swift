==============================================================
Configure project-specific image locations with Object Storage
==============================================================

For some deployers, it is not ideal to store all images in one place to
enable all projects and users to access them. You can configure the Image
service to store image data in project-specific image locations. Then,
only the following projects can use the Image service to access the
created image:

- The project who owns the image
- Projects that are defined in ``swift_store_admin_tenants`` and that
  have admin-level accounts

**To configure project-specific image locations**

#. Configure swift as your ``default_store`` in the
   ``glance-api.conf`` file.

#. Set these configuration options in the ``glance-api.conf`` file:

   - swift_store_multi_tenant
      Set to ``True`` to enable tenant-specific storage locations.
      Default is ``False``.

   - swift_store_admin_tenants
      Specify a list of tenant IDs that can grant read and write access to all
      Object Storage containers that are created by the Image service.

With this configuration, images are stored in an Object Storage service
(swift) endpoint that is pulled from the service catalog for the
authenticated user.
