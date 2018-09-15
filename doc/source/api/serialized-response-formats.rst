===========================
Serialized response formats
===========================

By default, the Object Storage API uses a ``text/plain`` response
format. In addition, both JSON and XML data serialization response
formats are supported.

To define the response format, use one of these methods:

+-------------------+-------------------------------------------------------+
|Method             |Description                                            |
+===================+=======================================================+
|format= ``format`` |Append this parameter to the URL for a ``GET`` request,|
|query parameter    |where ``format`` is ``json`` or ``xml``.               |
+-------------------+-------------------------------------------------------+
|``Accept`` request |Include this header in the ``GET`` request.            |
|header             |The valid header values are:                           |
|                   |                                                       |
|                   |text/plain                                             |
|                   |  Plain text response format. The default.             |
|                   |application/jsontext                                   |
|                   |  JSON data serialization response format.             |
|                   |application/xml                                        |
|                   |  XML data serialization response format.              |
|                   |text/xml                                               |
|                   |  XML data serialization response format.              |
+-------------------+-------------------------------------------------------+

Example 1. JSON example with format query parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For example, this request uses the ``format`` query parameter to ask
for a JSON response:

.. code-block:: console

   $ curl -i $publicURL?format=json -X GET -H "X-Auth-Token: $token"

.. code-block:: console

   HTTP/1.1 200 OK
   Content-Length: 96
   X-Account-Object-Count: 1
   X-Timestamp: 1389453423.35964
   X-Account-Meta-Subject: Literature
   X-Account-Bytes-Used: 14
   X-Account-Container-Count: 2
   Content-Type: application/json; charset=utf-8
   Accept-Ranges: bytes
   X-Trans-Id: tx274a77a8975c4a66aeb24-0052d95365
   Date: Fri, 17 Jan 2014 15:59:33 GMT

Object Storage lists container names with additional information in JSON
format:

.. code-block:: json

    [
       {
          "count":0,
          "bytes":0,
          "name":"janeausten"
       },
       {
          "count":1,
          "bytes":14,
          "name":"marktwain"
       }
    ]


Example 2. XML example with Accept header
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This request uses the ``Accept`` request header to ask for an XML
response:

.. code-block:: console

   $ curl -i $publicURL -X GET -H "X-Auth-Token: $token" -H \
     "Accept: application/xml; charset=utf-8"

.. code-block:: console

   HTTP/1.1 200 OK
   Content-Length: 263
   X-Account-Object-Count: 3
   X-Account-Meta-Book: MobyDick
   X-Timestamp: 1389453423.35964
   X-Account-Bytes-Used: 47
   X-Account-Container-Count: 2
   Content-Type: application/xml; charset=utf-8
   Accept-Ranges: bytes
   X-Trans-Id: txf0b4c9727c3e491694019-0052e03420
   Date: Wed, 22 Jan 2014 21:12:00 GMT

Object Storage lists container names with additional information in XML
format:

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
    <account name="AUTH_73f0aa26640f4971864919d0eb0f0880">
        <container>
            <name>janeausten</name>
            <count>2</count>
            <bytes>33</bytes>
        </container>
        <container>
            <name>marktwain</name>
            <count>1</count>
            <bytes>14</bytes>
        </container>
    </account>

The remainder of the examples in this guide use standard, non-serialized
responses. However, all ``GET`` requests that perform list operations
accept the ``format`` query parameter or ``Accept`` request header.
