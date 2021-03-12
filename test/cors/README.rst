CORS Functional Tests
=====================

`Cross Origin Resource Sharing <https://www.w3.org/TR/cors/>`__ is a bit
of a complicated beast. It focuses on the interactions between

* a **user-agent** (typically a web browser),
* a "**source origin**" server (whose code the user-agent is running), and
* some **other server** (for our purposes, usually Swift).

Where it gets hairy is that there may be varying degrees of trust between
these different actors.

Fortunately, Swift `allows per-container configuration
<https://docs.openstack.org/swift/latest/cors.html>`__ of many CORS options.
However, our normal functional tests only exercise bits and pieces of CORS,
without telling a complete story or performing a true end-to-end test. *These*
tests aim to remedy that.

The tests consist of three parts:

* setup
    Create several test containers with well-known names, set appropriate
    ACLs and CORS metadata, and upload some test objects.

* serve
    Serve a static website on localhost which, on load, will make several
    CORS requests and verify expected behavior.

* run
    Use Selenium to load the website, wait for and scrape the results, and
    output them in `TAP format <http://testanything.org/tap-specification.html>`__.
    Alternatively, open the page in your local browser and manually inspect whether
    tests passed or failed.

All of this is orchestrated through ``main.py``. It uses the standard ``OS_*``
environment variables to determine how to connect to Swift:

* ``OS_AUTH_URL`` (or ``ST_AUTH``)
* ``OS_USERNAME`` (or ``ST_USER``)
* ``OS_PASSWORD`` (or ``ST_KEY``)
* ``OS_STORAGE_URL`` (optional)

There are additional environment variables to exercise the S3 API:

* ``S3_ENDPOINT``
* ``S3_USER``
* ``S3_KEY``

.. note::
   It is necessary to set `s3_acl = False` in the `[filter:s3api]` section of
   your `proxy-server.conf` for all the s3 object tests to pass.

..
   TODO: verify that this works with Keystone

Running Tests Manually
----------------------

To inspect the test results in your local browser, run::

   $ ./test/cors/main.py --no-run

This will create some test containers and object in Swift, start a simple
static site, and emit a URL to visit to run the tests, like::

   Serving test at http://localhost:8000/#OS_AUTH_URL=http://saio/auth/v1.0&OS_USERNAME=test:tester&OS_PASSWORD=testing&OS_STORAGE_URL=http://saio/v1/AUTH_test&S3_ENDPOINT=http://saio&S3_USER=test%3Atester&S3_KEY=testing

.. note::
   You can use ``--hostname`` and ``--port`` to adjust the origin used.

Open the link. Toward the top of the page will be a status line; it will cycle
through the following states:

* Loading
* Starting jobs
* Waiting for jobs to finish
* Complete

When complete, it will also include a summary of the number of tests run as
well as pass/fail/skip counts. Below the status line will be a table of
individual tests with status, description, and additional information.

You can also run a single test by adding a ``&test=<name>`` query parameter.
For example::

   http://localhost:8000/#OS_AUTH_URL=http://saio/auth/v1.0&OS_USERNAME=test:tester&OS_PASSWORD=testing&OS_STORAGE_URL=http://saio/v1/AUTH_test&test=object%20-%20GET

will just run the test named ``object - GET``.

To stop the server, press ``^C``.

Running Tests with Selenium
---------------------------

`Selenium <https://www.selenium.dev/>`__ may be used to automate visiting the
static site, waiting for tests to run, and gathering results. See the
`installation instructions <https://selenium-python.readthedocs.io/installation.html>`__
for the Python bindings for more information about setting this up.

.. note::
   On Linux, you may want to use ``xvfb-run`` to have browsers use a virtual
   display.

When using selenium, the test runner will try to run tests in Firefox, Chrome,
Safari, Edge, and IE if available; if a browser seems to not be available, its
tests will be skipped.

Updating aws-sdk-js
-------------------

There are tests that exercise CORS over the S3 API; these use a vendored
version of `aws-sdk-js <https://github.com/aws/aws-sdk-js/>`__ that only
covers the S3 service. The current version used is 2.829.0, built on
2021-01-21 by

* visiting https://sdk.amazonaws.com/builder/js/,
* clearing all services,
* explicitly adding AWS.S3,
* clicking "Build" to download,
* saving in the ``test/cors/vendor`` directory, and finally
* updating the version number in ``test/cors/test-s3*.js``.
