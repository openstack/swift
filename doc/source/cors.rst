====
CORS
====

CORS_ is a mechanisim to allow code running in a browser (Javascript for
example) make requests to a domain other then the one from where it originated.

Swift supports CORS requests to containers and objects.

CORS metadata is held on the container only. The values given apply to the
container itself and all objects within it.

The supported headers are,

+---------------------------------------------+-------------------------------+
|Metadata                                      | Use                          |
+==============================================+==============================+
|X-Container-Meta-Access-Control-Allow-Origin  | Origins to be allowed to     |
|                                              | make Cross Origin Requests,  |
|                                              | space separated.             |
+----------------------------------------------+------------------------------+
|X-Container-Meta-Access-Control-Max-Age       | Max age for the Origin to    |
|                                              | hold the preflight results.  |
+----------------------------------------------+------------------------------+
|X-Container-Meta-Access-Control-Allow-Headers | Headers to be allowed in     |
|                                              | actual request by browser,   |
|                                              | space seperated.             |
+----------------------------------------------+------------------------------+
|X-Container-Meta-Access-Control-Expose-Headers| Headers exposed to the user  |
|                                              | agent (e.g. browser) in the  |
|                                              | the actual request response. |
|                                              | Space seperated.             |
+----------------------------------------------+------------------------------+

Before a browser issues an actual request it may issue a `preflight request`_.
The preflight request is an OPTIONS call to verify the Origin is allowed to
make the request. The sequence of events are,

* Browser makes OPTIONS request to Swift
* Swift returns 200/401 to browser based on allowed origins
* If 200, browser makes the "actual request" to Swift, i.e. PUT, POST, DELETE,
  HEAD, GET

When a browser receives a response to an actual request it only exposes those
headers listed in the ``Access-Control-Expose-Headers`` header. By default Swift
returns the following values for this header,

* "simple response headers" as listed on
  http://www.w3.org/TR/cors/#simple-response-header
* the headers ``etag``, ``x-timestamp``, ``x-trans-id``
* all metadata headers (``X-Container-Meta-*`` for containers and
  ``X-Object-Meta-*`` for objects)
* headers listed in ``X-Container-Meta-Access-Control-Expose-Headers``


-----------------
Sample Javascript
-----------------

To see some CORS Javascript in action download the `test CORS page`_ (source
below). Host it on a webserver and take note of the protocol and hostname
(origin) you'll be using to request the page, e.g. http://localhost.

Locate a container you'd like to query. Needless to say the Swift cluster
hosting this container should have CORS support. Append the origin of the
test page to the container's ``X-Container-Meta-Access-Control-Allow-Origin``
header,::

    curl -X POST -H 'X-Auth-Token: xxx' \
      -H 'X-Container-Meta-Access-Control-Allow-Origin: http://localhost' \
      http://192.168.56.3:8080/v1/AUTH_test/cont1

At this point the container is now accessable to CORS clients hosted on
http://localhost. Open the test CORS page in your browser.

#. Populate the Token field
#. Populate the URL field with the URL of either a container or object
#. Select the request method
#. Hit Submit

Assuming the request succeeds you should see the response header and body. If
something went wrong the response status will be 0.

.. _test CORS page:

Test CORS Page
--------------

::

    <!DOCTYPE html>
    <html>
      <head>
        <meta charset="utf-8">
        <title>Test CORS</title>
      </head>
      <body>

        Token<br><input id="token" type="text" size="64"><br><br>

        Method<br>
        <select id="method">
            <option value="GET">GET</option>
            <option value="HEAD">HEAD</option>
            <option value="POST">POST</option>
            <option value="DELETE">DELETE</option>
            <option value="PUT">PUT</option>
        </select><br><br>

        URL (Container or Object)<br><input id="url" size="64" type="text"><br><br>

        <input id="submit" type="button" value="Submit" onclick="submit(); return false;">

        <pre id="response_headers"></pre>
        <p>
        <hr>
        <pre id="response_body"></pre>

        <script type="text/javascript">
          function submit() {
              var token = document.getElementById('token').value;
              var method = document.getElementById('method').value;
              var url = document.getElementById('url').value;

              document.getElementById('response_headers').textContent = null;
              document.getElementById('response_body').textContent = null;

              var request = new XMLHttpRequest();

              request.onreadystatechange = function (oEvent) {
                  if (request.readyState == 4) {
                      responseHeaders = 'Status: ' + request.status;
                      responseHeaders = responseHeaders + '\nStatus Text: ' + request.statusText;
                      responseHeaders = responseHeaders + '\n\n' + request.getAllResponseHeaders();
                      document.getElementById('response_headers').textContent = responseHeaders;
                      document.getElementById('response_body').textContent = request.responseText;
                  }
              }

              request.open(method, url);
              request.setRequestHeader('X-Auth-Token', token);
              request.send(null);
          }
        </script>

      </body>
    </html>

.. _CORS: https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS
.. _preflight request: https://developer.mozilla.org/en-US/docs/HTTP/Access_control_CORS#Preflighted_requests

