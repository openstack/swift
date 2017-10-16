Swift3
------

Swift3 Middleware for OpenStack Swift, allowing access to OpenStack
swift via the Amazon S3 API.

Features
-------
 - Support AWS Signature Version 2, 4 (Version 4 is ready for only keystone)
 - Support Services APIs (GET)
 - Support Bucket APIs (GET/PUT/DELETE/HEAD)
 - Support Object APIs (GET/PUT/DELETE/HEAD)
 - Support Multipart Upload (required **SLO** middleware support)
 - Support S3 ACL (**under development**)

Install
-------

1) Install Swift3 with ``sudo python setup.py install`` or ``sudo python
   setup.py develop`` or via whatever packaging system you may be using.

2) Alter your proxy-server.conf pipeline to have swift3:

If you use tempauth:

    Was::

        [pipeline:main]
        pipeline = catch_errors cache tempauth proxy-server

    Change To::

        [pipeline:main]
        pipeline = catch_errors cache swift3 tempauth proxy-server

    To support Multipart Upload::

        [pipeline:main]
        pipeline = catch_errors cache swift3 tempauth slo proxy-server


If you use keystone:

    Was::

        [pipeline:main]
        pipeline = catch_errors cache authtoken keystone proxy-server

    Change To::

        [pipeline:main]
        pipeline = catch_errors cache authtoken swift3 s3token keystoneauth proxy-server

    To support Multipart Upload::

        [pipeline:main]
        pipeline = catch_errors cache authtoken swift3 s3token keystoneauth slo proxy-server

Note:
 * The authtoken filter requires the keystonemiddleware package.
 * Swift3 explicitly checks that keystoneauth is in the pipeline.  You must use this name
   in the pipeline statement and in [filter:keystoneauth] section header.
 * The authtoken filter is only required if Keystone users will use the native Swift API.

3) Add to your proxy-server.conf the section for the Swift3 WSGI filter::

    [filter:swift3]
    use = egg:swift3#swift3

You also need to add the following if you use keystone (adjust port, host, protocol configurations for your environment):

    [filter:s3token]
    use = egg:swift3#s3token
    auth_uri = http://127.0.0.1:35357/


4) Swift3 config options:

 You can find a proxy config example in `swift3/etc/proxy-server.conf-sample`.

    # Swift has no concept of the S3's resource owner; the resources
    # (i.e. containers and objects) created via the Swift API have no owner
    # information. This option specifies how the swift3 middleware handles them
    # with the S3 API.  If this option is 'false', such kinds of resources will be
    # invisible and no users can access them with the S3 API.  If set to 'true',
    # the resource without owner is belong to everyone and everyone can access it
    # with the S3 API.  If you care about S3 compatibility, set 'false' here.  This
    # option makes sense only when the s3_acl option is set to 'true' and your
    # Swift cluster has the resources created via the Swift API.
    allow_no_owner = false

    # Set a region name of your Swift cluster.  Note that Swift3 doesn't choose a
    # region of the newly created bucket actually.  This value is used only for the
    # GET Bucket location API.
    location = US

    # Set the default maximum number of objects returned in the GET Bucket
    # response.
    max_bucket_listing = 1000

    # Set the maximum number of objects we can delete with the Multi-Object Delete
    # operation.
    max_multi_delete_objects = 1000

    # If set to 'true', Swift3 uses its own metadata for ACL
    # (e.g. X-Container-Sysmeta-Swift3-Acl) to achieve the best S3 compatibility.
    # If set to 'false', Swift3 tries to use Swift ACL (e.g. X-Container-Read)
    # instead of S3 ACL as far as possible.  If you want to keep backward
    # compatibility with Swift3 1.7 or earlier, set false here
    # If set to 'false' after set to 'true' and put some container/object,
    # all users will be able to access container/object.
    # Note that s3_acl doesn't keep the acl consistency between S3 API and Swift
    # API. (e.g. when set s3acl to true and PUT acl, we won't get the acl
    # information via Swift API at all and the acl won't be applied against to
    # Swift API even if it is for a bucket currently supported.)
    # Note that s3_acl currently supports only keystone and tempauth.
    # DON'T USE THIS for production before enough testing for your use cases.
    # This stuff is still under development and it might cause something
    # you don't expect.
    s3_acl = false

    # Specify a host name of your Swift cluster.  This enables virtual-hosted style
    # requests.
    storage_domain =


Functional and Unit Tests
-------------------------
We provide functional/unit tests to make swift3 middleware more stable.

    For Ubuntu 12.04
    $ sudo apt-get install python-dev python-pip libffi-dev libssl-dev libxml2-dev libxslt1-dev
    $ sudo pip install tox

    # Run unit tests with Python 2.7
    $ tox -e py27

    # Run functional tests with tempauth
    $ tox -e tempauth

    # Run functional tests with s3token, keystoneauth and keystone
    $ tox -e keystone

    # Run all tests
    $ tox

    # For developer, please run unit tests and syntax check tests before summit path
    $ tox -e pylint -e pep8 -e py27
