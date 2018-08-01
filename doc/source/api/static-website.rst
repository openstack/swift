.. _static-website:

=====================
Create static website
=====================

To discover whether your Object Storage system supports this feature,
see :ref:`discoverability`. Alternatively, check with your service
provider.

You can use your Object Storage account to create a static website. This
static website is created with Static Web middleware and serves container
data with a specified index file, error file resolution, and optional
file listings. This mode is normally active only for anonymous requests,
which provide no authentication token. To use it with authenticated
requests, set the header ``X-Web-Mode`` to ``TRUE`` on the request.

The Static Web filter must be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file below any authentication
middleware. You must also add a Static Web middleware configuration
section.

Your publicly readable containers are checked for two headers,
``X-Container-Meta-Web-Index`` and ``X-Container-Meta-Web-Error``. The
``X-Container-Meta-Web-Error`` header is discussed below, in the
section called :ref:`set_error_static_website`.

Use ``X-Container-Meta-Web-Index`` to determine the index file (or
default page served, such as ``index.html``) for your website. When
someone initially enters your site, the ``index.html`` file displays
automatically. If you create sub-directories for your site by creating
pseudo-directories in your container, the index page for each
sub-directory is displayed by default. If your pseudo-directory does not
have a file with the same name as your index file, visits to the
sub-directory return a 404 error.

You also have the option of displaying a list of files in your
pseudo-directory instead of a web page. To do this, set the
``X-Container-Meta-Web-Listings`` header to ``TRUE``. You may add styles
to your file listing by setting ``X-Container-Meta-Web-Listings-CSS``
to a style sheet (for example, ``lists.css``).

Static Web middleware through Object Storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following sections show how to use Static Web middleware through
Object Storage.

Make container publicly readable
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Make the container publicly readable. Once the container is publicly
readable, you can access your objects directly, but you must set the
index file to browse the main site URL and its sub-directories.

.. code-block:: console

   $ swift post -r '.r:*,.rlistings' container


Set site index file
^^^^^^^^^^^^^^^^^^^

Set the index file. In this case, ``index.html`` is the default file
displayed when the site appears.

.. code-block:: console

   $ swift post -m 'web-index:index.html' container

Enable file listing
^^^^^^^^^^^^^^^^^^^

Turn on file listing. If you do not set the index file, the URL displays
a list of the objects in the container. Instructions on styling the list
with a CSS follow.

.. code-block:: console

   $ swift post -m 'web-listings: true' container

Enable CSS for file listing
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Style the file listing using a CSS.

.. code-block:: console

   $ swift post -m 'web-listings-css:listings.css' container

.. _set_error_static_website:

Set error pages for static website
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can create and set custom error pages for visitors to your website;
currently, only 401 (Unauthorized) and 404 (Not Found) errors are
supported. To do this, set the metadata header,
``X-Container-Meta-Web-Error``.

Error pages are served with the status code pre-pended to the name of
the error page you set. For instance, if you set
``X-Container-Meta-Web-Error`` to ``error.html``, 401 errors will
display the page ``401error.html``. Similarly, 404 errors will display
``404error.html``. You must have both of these pages created in your
container when you set the ``X-Container-Meta-Web-Error`` metadata, or
your site will display generic error pages.

You only have to set the ``X-Container-Meta-Web-Error`` metadata once
for your entire static website.

Set error pages for static website request
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: console

   $ swift post -m 'web-error:error.html' container


Any 2\ ``nn`` response indicates success.
