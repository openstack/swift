==================
General Procedures
==================

Getting a swift account stats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

   ``swift-direct`` is specific to the HPE Helion Public Cloud. Go look at
   ``swifty`` for an alternate, this is an example.

This procedure describes how you determine the swift usage for a given
swift account, that is the number of containers, number of objects and
total bytes used. To do this you will need the project ID.

Log onto one of the swift proxy servers.

Use swift-direct to show this accounts usage:

.. code::

   $ sudo -u swift /opt/hp/swift/bin/swift-direct show AUTH_redacted-9a11-45f8-aa1c-9e7b1c7904c8
   Status: 200
         Content-Length: 0
         Accept-Ranges: bytes
         X-Timestamp: 1379698586.88364
         X-Account-Bytes-Used: 67440225625994
         X-Account-Container-Count: 1
         Content-Type: text/plain; charset=utf-8
         X-Account-Object-Count: 8436776
         Status: 200
         name: my_container  count: 8436776  bytes: 67440225625994

This account has 1 container. That container has 8436776 objects. The
total bytes used is 67440225625994.