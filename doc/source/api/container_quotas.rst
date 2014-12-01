================
Container quotas
================

You can set quotas on the size and number of objects stored in a
container by setting the following metadata:

-  ``X-Container-Meta-Quota-Bytes``. The size, in bytes, of objects that
   can be stored in a container.

-  ``X-Container-Meta-Quota-Count``. The number of objects that can be
   stored in a container.

When you exceed a container quota, subsequent requests to create objects
fail with a 413 Request Entity Too Large error.

The Object Storage system uses an eventual consistency model. When you
create a new object, the container size and object count might not be
immediately updated. Consequently, you might be allowed to create
objects even though you have actually exceeded the quota.

At some later time, the system updates the container size and object
count to the actual values. At this time, subsequent requests fails. In
addition, if you are currently under the
``X-Container-Meta-Quota-Bytes`` limit and a request uses chunked
transfer encoding, the system cannot know if the request will exceed the
quota so the system allows the request. However, once the quota is
exceeded, any subsequent uploads that use chunked transfer encoding
fail.

