``object-server`` Metrics
=========================

=========================================  ====================================================
Metric Name                                Description
-----------------------------------------  ----------------------------------------------------
``object-server.quarantines``              Count of objects (files) found bad and moved to
                                           quarantine.
``object-server.async_pendings``           Count of container updates saved as async_pendings
                                           (may result from PUT or DELETE requests).
``object-server.POST.errors.timing``       Timing data for POST request errors: bad request,
                                           missing timestamp, delete-at in past, not mounted.
``object-server.POST.timing``              Timing data for each POST request not resulting in
                                           an error.
``object-server.PUT.errors.timing``        Timing data for PUT request errors: bad request,
                                           not mounted, missing timestamp, object creation
                                           constraint violation, delete-at in past.
``object-server.PUT.timeouts``             Count of object PUTs which exceeded max_upload_time.
``object-server.PUT.timing``               Timing data for each PUT request not resulting in an
                                           error.
``object-server.PUT.<device>.timing``      Timing data per kB transferred (ms/kB) for each
                                           non-zero-byte PUT request on each device.
                                           Monitoring problematic devices, higher is bad.
``object-server.GET.errors.timing``        Timing data for GET request errors: bad request,
                                           not mounted, header timestamps before the epoch,
                                           precondition failed.
                                           File errors resulting in a quarantine are not
                                           counted here.
``object-server.GET.timing``               Timing data for each GET request not resulting in an
                                           error.  Includes requests which couldn't find the
                                           object (including disk errors resulting in file
                                           quarantine).
``object-server.HEAD.errors.timing``       Timing data for HEAD request errors: bad request,
                                           not mounted.
``object-server.HEAD.timing``              Timing data for each HEAD request not resulting in
                                           an error.  Includes requests which couldn't find the
                                           object (including disk errors resulting in file
                                           quarantine).
``object-server.DELETE.errors.timing``     Timing data for DELETE request errors: bad request,
                                           missing timestamp, not mounted, precondition
                                           failed.  Includes requests which couldn't find or
                                           match the object.
``object-server.DELETE.timing``            Timing data for each DELETE request not resulting
                                           in an error.
``object-server.REPLICATE.errors.timing``  Timing data for REPLICATE request errors: bad
                                           request, not mounted.
``object-server.REPLICATE.timing``         Timing data for each REPLICATE request not resulting
                                           in an error.
=========================================  ====================================================
