=======================
Middleware and Metadata
=======================

----------------
Using Middleware
----------------

`Python WSGI Middleware`_ (or just "middleware") can be used to "wrap"
the request and response of a Python WSGI application (i.e. a webapp,
or REST/HTTP API), like Swift's WSGI servers (proxy-server,
account-server, container-server, object-server).  Swift uses middleware
to add (sometimes optional) behaviors to the Swift WSGI servers.

.. _Python WSGI Middleware: http://www.python.org/dev/peps/pep-0333/#middleware-components-that-play-both-sides

Middleware can be added to the Swift WSGI servers by modifying their
`paste`_ configuration file.  The majority of Swift middleware is applied
to the :ref:`proxy-server`.

.. _paste: http://pythonpaste.org/

Given the following basic configuration::

    [DEFAULT]
    log_level = DEBUG
    user = <your-user-name>

    [pipeline:main]
    pipeline = proxy-server

    [app:proxy-server]
    use = egg:swift#proxy

You could add the :ref:`healthcheck` middleware by adding a section for
that filter and adding it to the pipeline::

    [DEFAULT]
    log_level = DEBUG
    user = <your-user-name>

    [pipeline:main]
    pipeline = healthcheck proxy-server

    [filter:healthcheck]
    use = egg:swift#healthcheck

    [app:proxy-server]
    use = egg:swift#proxy


Some middleware is required and will be inserted into your pipeline
automatically by core swift code (e.g. the proxy-server will insert
:ref:`catch_errors` and :ref:`gatekeeper` at the start of the pipeline if they
are not already present).  You can see which features are available on a given
Swift endpoint (including middleware) using the :ref:`discoverability`
interface.


----------------------------
Creating Your Own Middleware
----------------------------

The best way to see how to write middleware is to look at examples.

Many optional features in Swift are implemented as
:ref:`common_middleware` and provided in ``swift.common.middleware``, but
Swift middleware may be packaged and distributed as a separate project.
Some examples are listed on the :ref:`associated_projects` page.

A contrived middleware example that modifies request behavior by
inspecting custom HTTP headers (e.g. X-Webhook) and uses :ref:`sysmeta`
to persist data to backend storage as well as common patterns like a
:func:`.get_container_info` cache/query and :func:`.wsgify` decorator is
presented below::

    from swift.common.http import is_success
    from swift.common.swob import wsgify
    from swift.common.utils import split_path, get_logger
    from swift.common.request_helpers import get_sys_meta_prefix
    from swift.proxy.controllers.base import get_container_info
    from eventlet import Timeout
    import six
    if six.PY3:
        from eventlet.green.urllib import request as urllib2
    else:
        from eventlet.green import urllib2

    # x-container-sysmeta-webhook
    SYSMETA_WEBHOOK = get_sys_meta_prefix('container') + 'webhook'


    class WebhookMiddleware(object):
        def __init__(self, app, conf):
            self.app = app
            self.logger = get_logger(conf, log_route='webhook')

        @wsgify
        def __call__(self, req):
            obj = None
            try:
                (version, account, container, obj) = \
                    split_path(req.path_info, 4, 4, True)
            except ValueError:
                # not an object request
                pass
            if 'x-webhook' in req.headers:
                # translate user's request header to sysmeta
                req.headers[SYSMETA_WEBHOOK] = \
                    req.headers['x-webhook']
            if 'x-remove-webhook' in req.headers:
                # empty value will tombstone sysmeta
                req.headers[SYSMETA_WEBHOOK] = ''
            # account and object storage will ignore x-container-sysmeta-*
            resp = req.get_response(self.app)
            if obj and is_success(resp.status_int) and req.method == 'PUT':
                container_info = get_container_info(req.environ, self.app)
                # container_info may have our new sysmeta key
                webhook = container_info['sysmeta'].get('webhook')
                if webhook:
                    # create a POST request with obj name as body
                    webhook_req = urllib2.Request(webhook, data=obj)
                    with Timeout(20):
                        try:
                            urllib2.urlopen(webhook_req).read()
                        except (Exception, Timeout):
                            self.logger.exception(
                                'failed POST to webhook %s' % webhook)
                        else:
                            self.logger.info(
                                'successfully called webhook %s' % webhook)
            if 'x-container-sysmeta-webhook' in resp.headers:
                # translate sysmeta from the backend resp to
                # user-visible client resp header
                resp.headers['x-webhook'] = resp.headers[SYSMETA_WEBHOOK]
            return resp


    def webhook_factory(global_conf, **local_conf):
        conf = global_conf.copy()
        conf.update(local_conf)

        def webhook_filter(app):
            return WebhookMiddleware(app, conf)
        return webhook_filter

In practice this middleware will call the URL stored on the container as
X-Webhook on all successful object uploads.

If this example was at ``<swift-repo>/swift/common/middleware/webhook.py`` -
you could add it to your proxy by creating a new filter section and
adding it to the pipeline::

    [DEFAULT]
    log_level = DEBUG
    user = <your-user-name>

    [pipeline:main]
    pipeline = healthcheck webhook proxy-server

    [filter:webhook]
    paste.filter_factory = swift.common.middleware.webhook:webhook_factory

    [filter:healthcheck]
    use = egg:swift#healthcheck

    [app:proxy-server]
    use = egg:swift#proxy

Most python packages expose middleware as entrypoints.  See `PasteDeploy`_
documentation for more information about the syntax of the ``use`` option.
All middleware included with Swift is installed to support the ``egg:swift``
syntax.

.. _PasteDeploy: http://pythonpaste.org/deploy/#egg-uris

Middleware may advertize its availability and capabilities via Swift's
:ref:`discoverability` support by using
:func:`.register_swift_info`::

    from swift.common.utils import register_swift_info
    def webhook_factory(global_conf, **local_conf):
        register_swift_info('webhook')
        def webhook_filter(app):
            return WebhookMiddleware(app)
        return webhook_filter


--------------
Swift Metadata
--------------

Generally speaking metadata is information about a resource that is
associated with the resource but is not the data contained in the
resource itself - which is set and retrieved via HTTP headers. (e.g. the
"Content-Type" of a Swift object that is returned in HTTP response
headers)

All user resources in Swift (i.e. account, container, objects) can have
user metadata associated with them.  Middleware may also persist custom
metadata to accounts and containers safely using System Metadata.  Some
core Swift features which predate sysmeta have added exceptions for
custom non-user metadata headers (e.g.  :ref:`acls`,
:ref:`large-objects`)

.. _usermeta:

^^^^^^^^^^^^^
User Metadata
^^^^^^^^^^^^^

User metadata takes the form of ``X-<type>-Meta-<key>: <value>``, where
``<type>`` depends on the resources type (i.e. Account, Container, Object)
and ``<key>`` and ``<value>`` are set by the client.

User metadata should generally be reserved for use by the client or
client applications.  A perfect example use-case for user metadata is
`python-swiftclient`_'s ``X-Object-Meta-Mtime`` which it stores on
object it uploads to implement its ``--changed`` option which will only
upload files that have changed since the last upload.

.. _python-swiftclient: https://github.com/openstack/python-swiftclient

New middleware should avoid storing metadata within the User Metadata
namespace to avoid potential conflict with existing user metadata when
introducing new metadata keys.  An example of legacy middleware that
borrows the user metadata namespace is :ref:`tempurl`.  An example of
middleware which uses custom non-user metadata to avoid the user
metadata namespace is :ref:`slo-doc`.

User metadata that is stored by a PUT or POST request to a container or account
resource persists until it is explicitly removed by a subsequent PUT or POST
request that includes a header ``X-<type>-Meta-<key>`` with no value or a
header ``X-Remove-<type>-Meta-<key>: <ignored-value>``. In the latter case the
``<ignored-value>`` is not stored. All user metadata stored with an account or
container resource is deleted when the account or container is deleted.

User metadata that is stored with an object resource has a different semantic;
object user metadata persists until any subsequent PUT or POST request is made
to the same object, at which point all user metadata stored with that object is
deleted en-masse and replaced with any user metadata included with the PUT or
POST request. As a result, it is not possible to update a subset of the user
metadata items stored with an object while leaving some items unchanged.

.. _sysmeta:

^^^^^^^^^^^^^^^
System Metadata
^^^^^^^^^^^^^^^

System metadata takes the form of ``X-<type>-Sysmeta-<key>: <value>``,
where ``<type>`` depends on the resources type (i.e. Account, Container,
Object) and ``<key>`` and ``<value>`` are set by trusted code running in a
Swift WSGI Server.

All headers on client requests in the form of ``X-<type>-Sysmeta-<key>``
will be dropped from the request before being processed by any
middleware.  All headers on responses from back-end systems in the form
of ``X-<type>-Sysmeta-<key>`` will be removed after all middlewares have
processed the response but before the response is sent to the client.
See :ref:`gatekeeper` middleware for more information.

System metadata provides a means to store potentially private custom
metadata with associated Swift resources in a safe and secure fashion
without actually having to plumb custom metadata through the core swift
servers.  The incoming filtering ensures that the namespace can not be
modified directly by client requests, and the outgoing filter ensures
that removing middleware that uses a specific system metadata key
renders it benign.  New middleware should take advantage of system
metadata.

System metadata may be set on accounts and containers by including headers with
a PUT or POST request. Where a header name matches the name of an existing item
of system metadata, the value of the existing item will be updated. Otherwise
existing items are preserved. A system metadata header with an empty value will
cause any existing item with the same name to be deleted.

System metadata may be set on objects using only PUT requests. All items of
existing system metadata will be deleted and replaced en-masse by any system
metadata headers included with the PUT request. System metadata is neither
updated nor deleted by a POST request: updating individual items of system
metadata with a POST request is not yet supported in the same way that updating
individual items of user metadata is not supported. In cases where middleware
needs to store its own metadata with a POST request, it may use Object Transient
Sysmeta.

.. _transient_sysmeta:

^^^^^^^^^^^^^^^^^^^^^^^^
Object Transient-Sysmeta
^^^^^^^^^^^^^^^^^^^^^^^^

If middleware needs to store object metadata with a POST request it may do so
using headers of the form ``X-Object-Transient-Sysmeta-<key>: <value>``.

All headers on client requests in the form of
``X-Object-Transient-Sysmeta-<key>`` will be dropped from the request before
being processed by any middleware.  All headers on responses from back-end
systems in the form of ``X-Object-Transient-Sysmeta-<key>`` will be removed
after all middlewares have processed the response but before the response is
sent to the client. See :ref:`gatekeeper` middleware for more information.

Transient-sysmeta updates on an object have the same semantic as user
metadata updates on an object (see :ref:`usermeta`) i.e. whenever any PUT or
POST request is made to an object, all existing items of transient-sysmeta are
deleted en-masse and replaced with any transient-sysmeta included with the PUT
or POST request. Transient-sysmeta set by a middleware is therefore prone to
deletion by a subsequent client-generated POST request unless the middleware is
careful to include its transient-sysmeta with every POST. Likewise, user
metadata set by a client is prone to deletion by a subsequent
middleware-generated POST request, and for that reason middleware should avoid
generating POST requests that are independent of any client request.

Transient-sysmeta deliberately uses a different header prefix to user metadata
so that middlewares can avoid potential conflict with user metadata keys.

Transient-sysmeta deliberately uses a different header prefix to system
metadata to emphasize the fact that the data is only persisted until a
subsequent POST.
