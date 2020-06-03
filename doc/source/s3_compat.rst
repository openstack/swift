S3/Swift REST API Comparison Matrix
===================================

General compatibility statement
-------------------------------

S3 is a product from Amazon, and as such, it includes "features" that
are  outside the scope of Swift itself. For example, Swift doesn't
have anything to do with billing, whereas S3 buckets can be tied to
Amazon's billing system. Similarly, log delivery is a service outside
of Swift. It's entirely possible for a Swift deployment to provide that
functionality, but it is not part of Swift itself. Likewise, a Swift
deployment can provide similar geographic availability as S3, but this
is tied to the deployer's willingness to build the infrastructure and
support systems to do so.

Amazon S3 operations
---------------------

+------------------------------------------------+------------------+--------------+
| S3 REST API method                             | Category         | Swift S3 API |
+================================================+==================+==============+
| `GET Object`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `HEAD Object`_                                 | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Object`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Object Copy`_                             | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `DELETE Object`_                               | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Initiate Multipart Upload`_                   | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Upload Part`_                                 | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Upload Part Copy`_                            | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Complete Multipart Upload`_                   | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Abort Multipart Upload`_                      | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `List Parts`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `GET Object ACL`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Object ACL`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Bucket`_                                  | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `GET Bucket List Objects`_                     | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `HEAD Bucket`_                                 | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `DELETE Bucket`_                               | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `List Multipart Uploads`_                      | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `GET Bucket acl`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `PUT Bucket acl`_                              | Core-API         | Yes          |
+------------------------------------------------+------------------+--------------+
| `Versioning`_                                  | Versioning       | Yes          |
+------------------------------------------------+------------------+--------------+
| `Bucket notification`_                         | Notifications    | No           |
+------------------------------------------------+------------------+--------------+
| Bucket Lifecycle [1]_ [2]_ [3]_ [4]_ [5]_ [6]_ | Bucket Lifecycle | No           |
+------------------------------------------------+------------------+--------------+
| `Bucket policy`_                               | Advanced ACLs    | No           |
+------------------------------------------------+------------------+--------------+
| Public website [7]_ [8]_ [9]_ [10]_            | Public Website   | No           |
+------------------------------------------------+------------------+--------------+
| Billing [11]_ [12]_                            | Billing          | No           |
+------------------------------------------------+------------------+--------------+
| `GET Bucket location`_                         | Advanced Feature | Yes          |
+------------------------------------------------+------------------+--------------+
| `Delete Multiple Objects`_                     | Advanced Feature | Yes          |
+------------------------------------------------+------------------+--------------+
| `Object tagging`_                              | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `GET Object torrent`_                          | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `Bucket inventory`_                            | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `GET Bucket service`_                          | Advanced Feature | No           |
+------------------------------------------------+------------------+--------------+
| `Bucket accelerate`_                           | CDN Integration  | No           |
+------------------------------------------------+------------------+--------------+

----

.. _GET Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectGET.html
.. _HEAD Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectHEAD.html
.. _PUT Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPUT.html
.. _PUT Object Copy: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectCOPY.html
.. _DELETE Object: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectDELETE.html
.. _Initiate Multipart Upload: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadInitiate.html
.. _Upload Part: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadUploadPart.html
.. _Upload Part Copy: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadUploadPartCopy.html
.. _Complete Multipart Upload: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadComplete.html
.. _Abort Multipart Upload: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadAbort.html
.. _List Parts: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadListParts.html
.. _GET Object ACL: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectGETacl.html
.. _PUT Object ACL: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPUTacl.html
.. _Delete Multiple Objects: http://docs.amazonwebservices.com/AmazonS3/latest/API/multiobjectdeleteapi.html
.. _GET Object torrent: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectGETtorrent.html
.. _Object tagging: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtagging.html

.. _PUT Bucket: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketPUT.html
.. _GET Bucket List Objects: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGET.html
.. _HEAD Bucket: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketHEAD.html
.. _DELETE Bucket: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketDELETE.html
.. _List Multipart Uploads: http://docs.amazonwebservices.com/AmazonS3/latest/API/mpUploadListMPUpload.html
.. _GET Bucket acl: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETacl.html
.. _PUT Bucket acl: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketPUTacl.html
.. _Bucket notification: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETnotification.html
.. _Bucket policy: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETpolicy.html
.. _GET Bucket location: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETlocation.html
.. _Bucket accelerate: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETaccelerate.html
.. _Bucket inventory: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETInventoryConfig.html
.. _GET Bucket service: http://docs.aws.amazon.com/AmazonS3/latest/API/RESTServiceGET.html

.. Versioning
.. _Versioning: http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETversioningStatus.html


.. Lifecycle
.. [1] `POST restore <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPOSTrestore.html>`_
.. [2] `Bucket lifecycle <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETlifecycle.html>`_
.. [3] `Bucket logging <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETlogging.html>`_
.. [4] `Bucket analytics <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETAnalyticsConfig.html>`_
.. [5] `Bucket metrics <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETMetricConfiguration.html>`_
.. [6] `Bucket replication <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETreplication.html>`_


.. Public website
.. [7] `OPTIONS object <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTOPTIONSobject.html>`_
.. [8] `Object POST from HTML form <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTObjectPOST.html>`_
.. [9] `Bucket public website <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTBucketGETwebsite.html>`_
.. [10] `Bucket CORS <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETcors.html>`_


.. Billing
.. [11] `Request payment <http://docs.amazonwebservices.com/AmazonS3/latest/API/RESTrequestPaymentPUT.html>`_
.. [12] `Bucket tagging <http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGETtagging.html>`_
