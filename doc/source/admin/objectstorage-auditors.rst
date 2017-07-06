==============
Object Auditor
==============

On system failures, the XFS file system can sometimes truncate files it is
trying to write and produce zero-byte files. The object-auditor will catch
these problems but in the case of a system crash it is advisable to run
an extra, less rate limited sweep, to check for these specific files.
You can run this command as follows:

.. code-block:: console

   $ swift-object-auditor /path/to/object-server/config/file.conf once -z 1000

.. note::

   "-z" means to only check for zero-byte files at 1000 files per second.

It is useful to run the object auditor on a specific device or set of devices.
You can run the object-auditor once as follows:

.. code-block:: console

   $ swift-object-auditor /path/to/object-server/config/file.conf once \
     --devices=sda,sdb

.. note::

   This will run the object auditor on only the ``sda`` and ``sdb`` devices.
   This parameter accepts a comma-separated list of values.
