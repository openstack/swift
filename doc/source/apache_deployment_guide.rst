=======================
Apache Deployment Guide
=======================

----------------------------
Web Front End Considerations
----------------------------

Swift can be configured to work both using an integral web front-end
and using a full-fledged Web Server such as the Apache2 (HTTPD) web server.
The integral web front-end is a wsgi mini "Web Server" which opens
up its own socket and serves http requests directly.
The incoming requests accepted by the integral web front-end are then forwarded
to a wsgi application (the core swift) for further handling, possibly
via wsgi middleware sub-components.

client<---->'integral web front-end'<---->middleware<---->'core swift'

To gain full advantage of Apache2, Swift can alternatively be
configured to work as a request processor of the Apache2 server.
This alternative deployment scenario uses mod_wsgi of Apache2
to forward requests to the swift wsgi application and middleware.

client<---->'Apache2 with mod_wsgi'<----->middleware<---->'core swift'

The integral web front-end offers simplicity and requires
minimal configuration. It is also the web front-end most commonly used
with Swift.
Additionally, the integral web front-end includes support for
receiving chunked transfer encoding from a client,
presently not supported by Apache2 in the operation mode described here.

The use of Apache2 offers new ways to extend Swift and integrate it with
existing authentication, administration and control systems.
A single Apache2 server can serve as the web front end of any number of swift
servers residing on a swift node.
For example when a storage node offers account, container and object services,
a single Apache2 server can serve as the web front end of all three services.

The apache variant described here was tested as part of an IBM research work.
It was found that following tuning, the Apache2 offer generally equivalent
performance to that offered by the integral web front-end.
Alternative to Apache2, other web servers may be used, but were never tested.

-------------
Apache2 Setup
-------------
Both Apache2 and mod-wsgi needs to be installed on the system.
Ubuntu comes with Apache2 installed. Install mod-wsgi using:: 

    sudo apt-get install libapache2-mod-wsgi

First, change the User and Group IDs of Apache2 to be those used by Swift.
For example in /etc/apache2/envvars use::

    export APACHE_RUN_USER=swift
    export APACHE_RUN_GROUP=swift

Create a directory for the Apache2 wsgi files::

    sudo mkdir /var/www/swift

Create a file for each service under /var/www/swift.

For a proxy service create /var/www/swift/proxy-server.wsgi::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/proxy-server.conf','proxy-server')

For an account service create /var/www/swift/account-server.wsgi::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/account-server.conf',
                               'account-server')

For an container service create /var/www/swift/container-server.wsgi::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/container-server.conf',
                              'container-server')

For an object service create /var/www/swift/object-server.wsgi::

    from swift.common.wsgi import init_request_processor
    application, conf, logger, log_name = \
        init_request_processor('/etc/swift/object-server.conf',
                               'object-server')

Create a /etc/apache2/conf.d/swift_wsgi.conf configuration file that will
define a port and Virtual Host per each local service.
For example an Apache2 serving as a web front end of a proxy service::

    #Proxy
    NameVirtualHost *:8080
    Listen 8080
    <VirtualHost *:8080>
        ServerName proxy-server
        LimitRequestBody 5368709122
        WSGIDaemonProcess proxy-server processes=5 threads=1
        WSGIProcessGroup proxy-server
        WSGIScriptAlias / /var/www/swift/proxy-server.wsgi
        LimitRequestFields 200
        ErrorLog /var/log/apache2/proxy-server
        LogLevel debug
        CustomLog /var/log/apache2/proxy.log combined
    </VirtualHost>

Notice that when using Apache the limit on the maximal object size should
be imposed by Apache using the LimitRequestBody rather by the swift proxy.
Note also that the LimitRequestBody should indicate the same value
as indicated by max_file_size located in both 
/etc/swift/swift.conf and in /etc/swift/test.conf.
The Swift default value for max_file_size (when not present) is 5368709122.
For example an Apache2 serving as a web front end of a storage node::

    #Object Service
    NameVirtualHost *:6200
    Listen 6200
    <VirtualHost *:6200>
        ServerName object-server
        WSGIDaemonProcess object-server processes=5 threads=1
        WSGIProcessGroup object-server
        WSGIScriptAlias / /var/www/swift/object-server.wsgi
        LimitRequestFields 200
        ErrorLog /var/log/apache2/object-server
        LogLevel debug
        CustomLog /var/log/apache2/access.log combined
    </VirtualHost>

    #Container Service
    NameVirtualHost *:6201
    Listen 6201
    <VirtualHost *:6201>
        ServerName container-server
        WSGIDaemonProcess container-server processes=5 threads=1
        WSGIProcessGroup container-server
        WSGIScriptAlias / /var/www/swift/container-server.wsgi
        LimitRequestFields 200
        ErrorLog /var/log/apache2/container-server
        LogLevel debug
        CustomLog /var/log/apache2/access.log combined
    </VirtualHost>

    #Account Service
    NameVirtualHost *:6202
    Listen 6202
    <VirtualHost *:6202>
        ServerName account-server
        WSGIDaemonProcess account-server processes=5 threads=1
        WSGIProcessGroup account-server
        WSGIScriptAlias / /var/www/swift/account-server.wsgi
        LimitRequestFields 200
        ErrorLog /var/log/apache2/account-server
        LogLevel debug
        CustomLog /var/log/apache2/access.log combined
    </VirtualHost>

Next stop the Apache2 and start it again (apache2ctl restart is not enough)::

    apache2ctl stop
    apache2ctl start

Edit the tests config file and add::

    web_front_end = apache2
    normalized_urls = True

Also check to see that the file includes max_file_size of the same value as
used for the LimitRequestBody in the apache config file above.

We are done.
You may run functional tests to test - e.g.::

    cd ~swift/swift
    ./.functests
