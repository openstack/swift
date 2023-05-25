``container-server`` Metrics
============================

.. note::
   "Not Found" is not considered an error and requests
   which increment ``errors`` are not included in the timing data.

============================================  ====================================================
Metric Name                                   Description
--------------------------------------------  ----------------------------------------------------
``container-server.DELETE.errors.timing``     Timing data for DELETE request errors: bad request,
                                              not mounted, missing timestamp, conflict.
``container-server.DELETE.timing``            Timing data for each DELETE request not resulting in
                                              an error.
``container-server.PUT.errors.timing``        Timing data for PUT request errors: bad request,
                                              missing timestamp, not mounted, conflict.
``container-server.PUT.timing``               Timing data for each PUT request not resulting in an
                                              error.
``container-server.HEAD.errors.timing``       Timing data for HEAD request errors: bad request,
                                              not mounted.
``container-server.HEAD.timing``              Timing data for each HEAD request not resulting in
                                              an error.
``container-server.GET.errors.timing``        Timing data for GET request errors: bad request,
                                              not mounted, parameters not utf8, bad accept header.
``container-server.GET.timing``               Timing data for each GET request not resulting in
                                              an error.
``container-server.REPLICATE.errors.timing``  Timing data for REPLICATE request errors: bad
                                              request, not mounted.
``container-server.REPLICATE.timing``         Timing data for each REPLICATE request not resulting
                                              in an error.
``container-server.POST.errors.timing``       Timing data for POST request errors: bad request,
                                              bad x-container-sync-to, not mounted.
``container-server.POST.timing``              Timing data for each POST request not resulting in
                                              an error.
============================================  ====================================================
