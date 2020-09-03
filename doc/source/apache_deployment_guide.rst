=======================
Apache Deployment Guide
=======================

----------------------------
Web Front End Considerations
----------------------------

Swift can be configured to work both using an integral web front-end and using a
full-fledged Web Server such as the Apache2 (HTTPD) web server. The integral
web front-end is a wsgi mini "Web Server" which opens up its own socket and
serves http requests directly. The incoming requests accepted by the integral
web front-end are then forwarded to a wsgi application (the core swift) for
further handling, possibly via wsgi middleware sub-components.

client<---->'integral web front-end'<---->middleware<---->'core swift'

To gain full advantage of Apache2, Swift can alternatively be configured to work
as a request processor of the Apache2 server. This alternative deployment
scenario uses mod_wsgi of Apache2 to forward requests to the swift wsgi
application and middleware.

client<---->'Apache2 with mod_wsgi'<----->middleware<---->'core swift'

The integral web front-end offers simplicity and requires minimal configuration.
It is also the web front-end most commonly used with Swift. Additionally, the
integral web front-end includes support for receiving chunked transfer encoding
from a client, presently not supported by Apache2 in the operation mode
described here.

The use of Apache2 offers new ways to extend Swift and integrate it with
existing authentication, administration and control systems. A single Apache2
server can serve as the web front end of any number of swift servers residing on
a swift node. For example when a storage node offers account, container and
object services, a single Apache2 server can serve as the web front end of all
three services.

The apache variant described here was tested as part of an IBM research work.
It was found that following tuning, the Apache2 offer generally equivalent
performance to that offered by the integral web front-end. Alternative to
Apache2, other web servers may be used, but were never tested.

-------------
Apache2 Setup
-------------
Both Apache2 and mod-wsgi needs to be installed on the system. Ubuntu comes
with Apache2 installed. Install mod-wsgi using::

    sudo apt-get install libapache2-mod-wsgi

Create a directory for the Apache2 wsgi files::

    sudo mkdir /srv/www/swift

Create a working directory for the wsgi processes::

    sudo mkdir -m 2770 /var/lib/swift
    sudo chown swift:swift /var/lib/swift

Create a file for each service under ``/srv/www/swift``.

For a proxy service create ``/srv/www/swift/proxy-server.wsgi``::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/proxy-server.conf','proxy-server')

For an account service create ``/srv/www/swift/account-server.wsgi``::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/account-server.conf',
                               'account-server')

For an container service create ``/srv/www/swift/container-server.wsgi``::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/container-server.conf',
                              'container-server')

For an object service create ``/srv/www/swift/object-server.wsgi``::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/object-server.conf',
                               'object-server')

Create a ``/etc/apache2/conf.d/swift_wsgi.conf`` configuration file that will
define a port and Virtual Host per each local service. For example an Apache2
serving as a web front end of a proxy service::

    # Proxy
    Listen 8080

    <VirtualHost *:8080>
        ServerName proxy-server

        LimitRequestBody 5368709122
        LimitRequestFields 200

        WSGIDaemonProcess proxy-server processes=5 threads=1 user=swift group=swift display-name=%{GROUP}
        WSGIProcessGroup proxy-server
        WSGIScriptAlias / /srv/www/swift/proxy-server.wsgi
        LogLevel debug
        CustomLog /var/log/apache2/proxy.log combined
        ErrorLog /var/log/apache2/proxy-server
    </VirtualHost>

Notice that when using Apache the limit on the maximal object size should be
imposed by Apache using the `LimitRequestBody` rather by the swift proxy. Note
also that the `LimitRequestBody` should indicate the same value as indicated by
`max_file_size` located in both ``/etc/swift/swift.conf`` and in
``/etc/swift/test.conf``.  The Swift default value for `max_file_size` (when not
present) is `5368709122`. For example an Apache2 serving as a web front end of a
storage node::

    # Object Service
    Listen 6200

    <VirtualHost *:6200>
        ServerName object-server

        LimitRequestFields 200

        WSGIDaemonProcess object-server processes=5 threads=1 user=swift group=swift display-name=%{GROUP}
        WSGIProcessGroup object-server
        WSGIScriptAlias / /srv/www/swift/object-server.wsgi
        LogLevel debug
        CustomLog /var/log/apache2/access.log combined
        ErrorLog /var/log/apache2/object-server
    </VirtualHost>

    # Container Service
    Listen 6201

    <VirtualHost *:6201>
        ServerName container-server

        LimitRequestFields 200

        WSGIDaemonProcess container-server processes=5 threads=1 user=swift group=swift display-name=%{GROUP}
        WSGIProcessGroup container-server
        WSGIScriptAlias / /srv/www/swift/container-server.wsgi
        LogLevel debug
        CustomLog /var/log/apache2/access.log combined
        ErrorLog /var/log/apache2/container-server
    </VirtualHost>

    # Account Service
    Listen 6202

    <VirtualHost *:6202>
        ServerName account-server

        LimitRequestFields 200

        WSGIDaemonProcess account-server processes=5 threads=1 user=swift group=swift display-name=%{GROUP}
        WSGIProcessGroup account-server
        WSGIScriptAlias / /srv/www/swift/account-server.wsgi
        LogLevel debug
        CustomLog /var/log/apache2/access.log combined
        ErrorLog /var/log/apache2/account-server
    </VirtualHost>

Enable the newly configured Virtual Hosts::

    a2ensite swift_wsgi.conf

Next, stop, test and start Apache2 again::

    # stop it
    systemctl stop apache2.service

    # test the configuration
    apache2ctl -t

    # start it if the test succeeds
    systemctl start apache2.service


Edit the tests config file and add::

    web_front_end = apache2
    normalized_urls = True

Also check to see that the file includes `max_file_size` of the same value as
used for the `LimitRequestBody` in the apache config file above.

We are done. You may run functional tests to test - e.g.::

    cd ~swift/swift
    ./.functests
