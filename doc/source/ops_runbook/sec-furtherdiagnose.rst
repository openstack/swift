==============================
Further issues and resolutions
==============================

.. note::

   The urgency levels in each **Action** column indicates whether or
   not it is required to take immediate action, or if the problem can be worked
   on during business hours.

.. list-table::
   :widths: 33 33 33
   :header-rows: 1

   * - **Scenario**
     - **Description**
     - **Action**
   * - ``/healthcheck`` latency is high.
     - The ``/healthcheck`` test does not tax the proxy very much so any drop in value is probably related to
       network issues, rather than the proxies being very busy. A very slow proxy might impact the average
       number, but it would need to be very slow to shift the number that much.
     - Check networks. Do a ``curl https://<ip-address>/healthcheck where ip-address`` is individual proxy
       IP address to see if you can pin point a problem in the network.

       Urgency: If there are other indications that your system is slow, you should treat
       this as an urgent problem.
   * - Swift process is not running.
     - You can use ``swift-init`` status to check if swift processes are running on any
       given server.
     - Run this command:
       .. code::

          sudo swift-init all start

       Examine messages in the swift log files to see if there are any
       error messages related to any of the swift processes since the time you
       ran the ``swift-init`` command.

       Take any corrective actions that seem necessary.

       Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - ntpd is not running.
     - NTP is not running.
     - Configure and start NTP.
       Urgency: For proxy servers, this is vital.

   * - Host clock is not syncd to an NTP server.
     - Node time settings does not match NTP server time.
       This may take some time to sync after a reboot.
     - Assuming NTP is configured and running, you have to wait until the times sync.
   * - A swift process has hundreds, to thousands of open file descriptors.
     - May happen to any of the swift processes.
       Known to have happened with a ``rsyslod restart`` and where ``/tmp`` was hanging.

     - Restart the swift processes on the affected node:

       .. code::

          % sudo swift-init all reload

       Urgency:
                If known performance problem: Immediate

                If system seems fine: Medium
   * - A swift process is not owned by the swift user.
     - If the UID of the swift user has changed, then the processes might not be
       owned by that UID.
     - Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - Object account or container files not owned by swift.
     - This typically happens if during a reinstall or a re-image of a server that the UID
       of the swift user was changed. The data files in the object account and container
       directories are owned by the original swift UID. As a result, the current swift
       user does not own these files.
     - Correct the UID of the swift user to reflect that of the original UID. An alternate
       action is to change the ownership of every file on all file systems. This alternate
       action is often impractical and will take considerable time.

       Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - A disk drive has a high IO wait or service time.
     - If high wait IO times are seen for a single disk, then the disk drive is the problem.
       If most/all devices are slow, the controller is probably the source of the problem.
       The controller cache may also be miss configured – which will cause similar long
       wait or service times.
     - As a first step, if your controllers have a cache, check that it is enabled and their battery/capacitor
       is working.

       Second, reboot the server.
       If problem persists, file a DC ticket to have the drive or controller replaced.
       See `Diagnose: Slow disk devices` on how to check the drive wait or service times.

       Urgency: Medium
   * - The network interface is not up.
     - Use the ``ifconfig`` and ``ethtool`` commands to determine the network state.
     - You can try restarting the interface. However, generally the interface
       (or cable) is probably broken, especially if the interface is flapping.

       Urgency: If this only affects one server, and you have more than one,
       identifying and fixing the problem can wait until business hours.
       If this same problem affects many servers, then you need to take corrective
       action immediately.
   * - Network interface card (NIC) is not operating at the expected speed.
     - The NIC is running at a slower speed than its nominal rated speed.
       For example, it is running at 100 Mb/s and the NIC is a 1Ge NIC.
     - 1. Try resetting the interface with:

       .. code::

          sudo ethtool -s eth0 speed 1000

       ... and then run:

       .. code::

          sudo lshw -class

       See if size goes to the expected speed. Failing
       that, check hardware (NIC cable/switch port).

       2. If persistent, consider shutting down the server (especially if a proxy)
          until the problem is identified and resolved. If you leave this server
          running it can have a large impact on overall performance.

       Urgency: High
   * - The interface RX/TX error count is non-zero.
     - A value of 0 is typical, but counts of 1 or 2 do not indicate a problem.
     - 1. For low numbers (For example, 1 or 2), you can simply ignore. Numbers in the range
          3-30 probably indicate that the error count has crept up slowly over a long time.
          Consider rebooting the server to remove the report from the noise.

          Typically, when a cable or interface is bad, the error count goes to 400+. For example,
          it stands out. There may be other symptoms such as the interface going up and down or
          not running at correct speed. A server with a high error count should be watched.

       2. If the error count continue to climb, consider taking the server down until
          it can be properly investigated. In any case, a reboot should be done to clear
          the error count.

       Urgency: High, if the error count increasing.

   * - In a swift log you see a message that a process has not replicated in over 24 hours.
     - The replicator has not successfully completed a run in the last 24 hours.
       This indicates that the replicator has probably hung.
     - Use ``swift-init`` to stop and then restart the replicator process.

       Urgency: Low (high if recent adding or replacement of disk drives), however if you
       recently added or replaced disk drives then you should treat this urgently.
   * - Container Updater has not run in 4 hour(s).
     - The service may appear to be running however, it may be hung. Examine their swift
       logs to see if there are any error messages relating to the container updater. This
       may potentially explain why the container is not running.
     - Urgency: Medium
       This may have been triggered by a recent restart of the  rsyslog daemon.
       Restart the service with:
       .. code::

          sudo swift-init <service> reload
   * - Object replicator: Reports the remaining time and that time is more than 100 hours.
     - Each replication cycle the object replicator writes a log message to its log
       reporting statistics about the current cycle. This includes an estimate for the
       remaining time needed to replicate all objects. If this time is longer than
       100 hours, there is a problem with the replication process.
     - Urgency: Medium
       Restart the service with:
       .. code::

          sudo swift-init object-replicator reload

       Check that the remaining replication time is going down.
