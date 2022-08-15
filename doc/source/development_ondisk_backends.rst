===============================
Pluggable On-Disk Back-end APIs
===============================

The internal REST API used between the proxy server and the account, container
and object server is almost identical to public Swift REST API, but with a few
internal extensions (for example, update an account with a new container).

The pluggable back-end APIs for the three REST API servers (account,
container, object) abstracts the needs for servicing the various REST APIs
from the details of how data is laid out and stored on-disk.

The APIs are documented in the reference implementations for all three
servers. For historical reasons, the object server backend reference
implementation module is named ``diskfile``, while the account and container
server backend reference implementation modules are named appropriately.

This API is still under development and not yet finalized.

-----------------------------------------
Back-end API for Account Server REST APIs
-----------------------------------------
.. automodule:: swift.account.backend
    :noindex:
    :members:

-------------------------------------------
Back-end API for Container Server REST APIs
-------------------------------------------
.. automodule:: swift.container.backend
    :noindex:
    :members:

----------------------------------------
Back-end API for Object Server REST APIs
----------------------------------------
.. automodule:: swift.obj.diskfile
    :noindex:
    :members:
