===========================
Troubleshoot Object Storage
===========================

For Object Storage, everything is logged in ``/var/log/syslog`` (or
``messages`` on some distros). Several settings enable further
customization of logging, such as ``log_name``, ``log_facility``, and
``log_level``, within the object server configuration files.

Drive failure
~~~~~~~~~~~~~

Problem
-------

Drive failure can prevent Object Storage performing replication.

Solution
--------

In the event that a drive has failed, the first step is to make sure the
drive is unmounted. This will make it easier for Object Storage to work
around the failure until it has been resolved. If the drive is going to
be replaced immediately, then it is just best to replace the drive,
format it, remount it, and let replication fill it up.

If you cannot replace the drive immediately, then it is best to leave it
unmounted, and remove the drive from the ring. This will allow all the
replicas that were on that drive to be replicated elsewhere until the
drive is replaced. Once the drive is replaced, it can be re-added to the
ring.

You can look at error messages in the ``/var/log/kern.log`` file for
hints of drive failure.

Server failure
~~~~~~~~~~~~~~

Problem
-------

The server is potentially offline, and may have failed, or require a
reboot.

Solution
--------

If a server is having hardware issues, it is a good idea to make sure
the Object Storage services are not running. This will allow Object
Storage to work around the failure while you troubleshoot.

If the server just needs a reboot, or a small amount of work that should
only last a couple of hours, then it is probably best to let Object
Storage work around the failure and get the machine fixed and back
online. When the machine comes back online, replication will make sure
that anything that is missing during the downtime will get updated.

If the server has more serious issues, then it is probably best to
remove all of the server's devices from the ring. Once the server has
been repaired and is back online, the server's devices can be added back
into the ring. It is important that the devices are reformatted before
putting them back into the ring as it is likely to be responsible for a
different set of partitions than before.

Detect failed drives
~~~~~~~~~~~~~~~~~~~~

Problem
-------

When drives fail, it can be difficult to detect that a drive has failed,
and the details of the failure.

Solution
--------

It has been our experience that when a drive is about to fail, error
messages appear in the ``/var/log/kern.log`` file. There is a script called
``swift-drive-audit`` that can be run via cron to watch for bad drives. If
errors are detected, it will unmount the bad drive, so that Object
Storage can work around it. The script takes a configuration file with
the following settings:

.. list-table:: **Description of configuration options for [drive-audit] in drive-audit.conf**
   :header-rows: 1

   * - Configuration option = Default value
     - Description
   * - ``device_dir = /srv/node``
     - Directory devices are mounted under
   * - ``error_limit = 1``
     - Number of errors to find before a device is unmounted
   * - ``log_address = /dev/log``
     - Location where syslog sends the logs to
   * - ``log_facility = LOG_LOCAL0``
     - Syslog log facility
   * - ``log_file_pattern = /var/log/kern.*[!.][!g][!z]``
     - Location of the log file with globbing pattern to check against device
       errors locate device blocks with errors in the log file
   * - ``log_level = INFO``
     - Logging level
   * - ``log_max_line_length = 0``
     - Caps the length of log lines to the value given; no limit if set to 0,
       the default.
   * - ``log_to_console = False``
     - No help text available for this option.
   * - ``minutes = 60``
     - Number of minutes to look back in ``/var/log/kern.log``
   * - ``recon_cache_path = /var/cache/swift``
     - Directory where stats for a few items will be stored
   * - ``regex_pattern_1 = \berror\b.*\b(dm-[0-9]{1,2}\d?)\b``
     - No help text available for this option.
   * - ``unmount_failed_device = True``
     - No help text available for this option.

.. warning::

   This script has only been tested on Ubuntu 10.04; use with caution on
   other operating systems in production.

Emergency recovery of ring builder files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Problem
-------

An emergency might prevent a successful backup from restoring the
cluster to operational status.

Solution
--------

You should always keep a backup of swift ring builder files. However, if
an emergency occurs, this procedure may assist in returning your cluster
to an operational state.

Using existing swift tools, there is no way to recover a builder file
from a ``ring.gz`` file. However, if you have a knowledge of Python, it
is possible to construct a builder file that is pretty close to the one
you have lost.

.. warning::

   This procedure is a last-resort for emergency circumstances. It
   requires knowledge of the swift python code and may not succeed.

#. Load the ring and a new ringbuilder object in a Python REPL:

   .. code-block:: python

      >>> from swift.common.ring import RingData, RingBuilder
      >>> ring = RingData.load('/path/to/account.ring.gz')

#. Start copying the data we have in the ring into the builder:

   .. code-block:: python

      >>> import math
      >>> partitions = len(ring._replica2part2dev_id[0])
      >>> replicas = len(ring._replica2part2dev_id)

      >>> builder = RingBuilder(int(math.log(partitions, 2)), replicas, 1)
      >>> builder.devs = ring.devs
      >>> builder._replica2part2dev = ring._replica2part2dev_id
      >>> builder._last_part_moves_epoch = 0
      >>> from array import array
      >>> builder._last_part_moves = array('B', (0 for _ in range(partitions)))
      >>> builder._set_parts_wanted()
      >>> for d in builder._iter_devs():
                  d['parts'] = 0
      >>> for p2d in builder._replica2part2dev:
                  for dev_id in p2d:
                      builder.devs[dev_id]['parts'] += 1

      This is the extent of the recoverable fields.

#. For ``min_part_hours`` you either have to remember what the value you
   used was, or just make up a new one:

   .. code-block:: python

      >>> builder.change_min_part_hours(24) # or whatever you want it to be

#. Validate the builder. If this raises an exception, check your
   previous code:

   .. code-block:: python

      >>> builder.validate()

#. After it validates, save the builder and create a new ``account.builder``:

   .. code-block:: python

      >>> import pickle
      >>> pickle.dump(builder.to_dict(), open('account.builder', 'wb'), protocol=2)
      >>> exit ()

#. You should now have a file called ``account.builder`` in the current
   working directory. Run
   :command:`swift-ring-builder account.builder write_ring` and compare the new
   ``account.ring.gz`` to the ``account.ring.gz`` that you started
   from. They probably are not byte-for-byte identical, but if you load them
   in a REPL and their ``_replica2part2dev_id`` and ``devs`` attributes are
   the same (or nearly so), then you are in good shape.

#. Repeat the procedure for ``container.ring.gz`` and
   ``object.ring.gz``, and you might get usable builder files.
