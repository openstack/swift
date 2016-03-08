=================
Swift Ops Runbook
=================

This document contains operational procedures that Hewlett Packard Enterprise (HPE) uses to operate
and monitor the Swift system within the HPE Helion Public Cloud. This
document is an excerpt of a larger product-specific handbook. As such,
the material may appear incomplete. The suggestions and recommendations
made in this document are for our particular environment, and may not be
suitable for your environment or situation. We make no representations
concerning the accuracy, adequacy, completeness or suitability of the
information, suggestions or recommendations. This document are provided
for reference only. We are not responsible for your use of any
information, suggestions or recommendations contained herein.

This document also contains references to certain tools that we use to
operate the Swift system within the HPE Helion Public Cloud.
Descriptions of these tools are provided for reference only, as the tools themselves
are not publically available at this time.

-  ``swift-direct``: This is similar to the ``swiftly`` tool.


.. toctree::
   :maxdepth: 2

   general.rst
   diagnose.rst
   procedures.rst
   maintenance.rst
   troubleshooting.rst

Is the system up?
~~~~~~~~~~~~~~~~~

If you have a report that Swift is down, perform the following basic checks:

#. Run swift functional tests.

#. From a server in your data center, use ``curl`` to check ``/healthcheck``.

#. If you have a monitoring system, check your monitoring system.

#. Check on your hardware load balancers infrastructure.

#. Run swift-recon on a proxy node.

Run swift function tests
------------------------

We would recommend that you set up your function tests against your production
system.

A script for running the function tests is located in ``swift/.functests``.


External monitoring
-------------------

-  We use pingdom.com to monitor the external Swift API. We suggest the
   following:

   -  Do a GET on ``/healthcheck``

   -  Create a container, make it public (x-container-read:
      .r\*,.rlistings), create a small file in the container; do a GET
      on the object

Reference information
~~~~~~~~~~~~~~~~~~~~~

Reference: Swift startup/shutdown
---------------------------------

-  Use reload - not stop/start/restart.

-  Try to roll sets of servers (especially proxy) in groups of less
   than 20% of your servers.

