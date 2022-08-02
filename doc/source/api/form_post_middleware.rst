====================
Form POST middleware
====================

To discover whether your Object Storage system supports this feature,
check with your service provider or send a **GET** request using the :file:`/info`
path.

You can upload objects directly to the Object Storage system from a
browser by using the form **POST** middleware. This middleware uses
account or container secret keys to generate a cryptographic signature for the
request. This means that you do not need to send an authentication token
in the ``X-Auth-Token`` header to perform the request.

The form **POST** middleware uses the same secret keys as the temporary
URL middleware uses. For information about how to set these keys, see
:ref:`secret_keys`.

For information about the form **POST** middleware configuration
options, see :ref:`formpost` in the *Source Documentation*.

Form POST format
~~~~~~~~~~~~~~~~

To upload objects to a cluster, you can use an HTML form **POST**
request.

The format of the form **POST** request is:

**Example 1.14. Form POST format**

.. code:: xml

    <form action="SWIFT_URL"
        method="POST"
        enctype="multipart/form-data">
        <input type="hidden" name="redirect" value="REDIRECT_URL"/>
        <input type="hidden" name="max_file_size" value="BYTES"/>
        <input type="hidden" name="max_file_count" value="COUNT"/>
        <input type="hidden" name="expires" value="UNIX_TIMESTAMP"/>
        <input type="hidden" name="signature" value="HMAC"/>
        <input type="file" name="FILE_NAME"/>
        <br/>
        <input type="submit"/>
    </form>


**action="SWIFT_URL"**

Set to full URL where the objects are to be uploaded. The names of
uploaded files are appended to the specified *SWIFT_URL*. So, you
can upload directly to the root of a container with a URL like:

.. code:: none

    https://swift-cluster.example.com/v1/my_account/container/

Optionally, you can include an object prefix to separate uploads, such
as:

.. code:: none

    https://swift-cluster.example.com/v1/my_account/container/OBJECT_PREFIX


**method="POST"**

Must be ``POST``.


**enctype="multipart/form-data"**

Must be ``multipart/form-data``.


**name="redirect" value="REDIRECT_URL"**

Redirects the browser to the *REDIRECT_URL* after the upload
completes. The URL has status and message query parameters added to it,
which specify the HTTP status code for the upload and an optional error
message. The 2\ *nn* status code indicates success.

The *REDIRECT_URL* can be an empty string. If so, the ``Location``
response header is not set.

**name="max\_file\_size" value="BYTES"**

Required. Indicates the size, in bytes, of the maximum single file
upload.

**name="max\_file\_count" value= "COUNT"**

Required. Indicates the maximum number of files that can be uploaded
with the form.


**name="expires" value="UNIX_TIMESTAMP"**

The UNIX timestamp that specifies the time before which the form must be
submitted before it becomes no longer valid.


**name="signature" value="HMAC"**

The HMAC-SHA1 signature of the form.


**type="file" name="FILE_NAME"**

File name of the file to be uploaded. You can include from one to the
``max_file_count`` value of files.

The file attributes must appear after the other attributes to be
processed correctly.

If attributes appear after the file attributes, they are not sent with
the sub-request because all attributes in the file cannot be parsed on
the server side unless the whole file is read into memory; the server
does not have enough memory to service these requests. Attributes that
follow the file attributes are ignored.

Optionally, if you want the uploaded files to be temporary you can set x-delete-at or x-delete-after attributes by adding one of these as a form input:

.. code:: xml

    <input type="hidden" name="x_delete_at" value="<unix-timestamp>" />
    <input type="hidden" name="x_delete_after" value="<seconds>" />


**type= "submit"**

Must be ``submit``.

HMAC-SHA1 signature for form POST
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Form **POST** middleware uses an HMAC-SHA1 cryptographic signature. This
signature includes these elements from the form:

-  The path. Starting with ``/v1/`` onwards and including a container
   name and, optionally, an object prefix. In `Example 1.15`, "HMAC-SHA1
   signature for form
   POST" the path is
   ``/v1/my_account/container/object_prefix``. Do not URL-encode the
   path at this stage.

-  A redirect URL. If there is no redirect URL, use the empty string.

-  Maximum file size. In `Example 1.15`, "HMAC-SHA1 signature for form
   POST" the
   ``max_file_size`` is ``104857600`` bytes.

-  The maximum number of objects to upload. In `Example 1.15`, "HMAC-SHA1
   signature for form
   POST" ``max_file_count`` is ``10``.

-  Expiry time. In `Example 1.15, "HMAC-SHA1 signature for form
   POST" the expiry time
   is set to ``600`` seconds into the future.

-  The secret key. Set as the ``X-Account-Meta-Temp-URL-Key`` header
   value for accounts or ``X-Container-Meta-Temp-URL-Key`` header
   value for containers.  See :ref:`secret_keys` for more information.

The following example code generates a signature for use with form
**POST**:

**Example 1.15. HMAC-SHA1 signature for form POST**

.. code:: python

    import hmac
    from hashlib import sha1
    from time import time
    path = '/v1/my_account/container/object_prefix'
    redirect = 'https://myserver.com/some-page'
    max_file_size = 104857600
    max_file_count = 10
    expires = int(time() + 600)
    key = 'MYKEY'
    hmac_body = '%s\n%s\n%s\n%s\n%s' % (path, redirect,
    max_file_size, max_file_count, expires)
    signature = hmac.new(key, hmac_body, sha1).hexdigest()


For more information, see `RFC 2104: HMAC: Keyed-Hashing for Message
Authentication <http://www.ietf.org/rfc/rfc2104.txt>`__.

Form POST example
~~~~~~~~~~~~~~~~~

The following example shows how to submit a form by using a cURL
command. In this example, the object prefix is ``photos/`` and the file
being uploaded is called ``flower.jpg``.

This example uses the **swift-form-signature** script to compute the
``expires`` and ``signature`` values.

.. code:: console

    $ bin/swift-form-signature /v1/my_account/container/photos/ https://example.com/done.html 5373952000 1 200 MYKEY
    Expires: 1390825338
    Signature: 35129416ebda2f1a21b3c2b8939850dfc63d8f43

.. code:: console

    $ curl -i https://swift-cluster.example.com/v1/my_account/container/photos/ -X POST \
           -F max_file_size=5373952000 -F max_file_count=1 -F expires=1390825338 \
           -F signature=35129416ebda2f1a21b3c2b8939850dfc63d8f43 \
           -F redirect=https://example.com/done.html \
           -F file=@flower.jpg
