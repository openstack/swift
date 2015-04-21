=======================
SAIO - Swift All In One
=======================

---------------------------------------------
Instructions for setting up a development VM
---------------------------------------------

This section documents setting up a virtual machine for doing Swift
development.  The virtual machine will emulate running a four node Swift
cluster.

* Get an Ubuntu 14.04 LTS server image or try something
  Fedora/CentOS.

* Create guest virtual machine from the image.

----------------------------
What's in a <your-user-name>
----------------------------

Much of the configuration described in this guide requires escalated
administrator (``root``) privileges; however, we assume that administrator logs
in as an unprivileged user and can use ``sudo`` to run privileged commands.

Swift processes also run under a separate user and group, set by configuration
option, and referenced as ``<your-user-name>:<your-group-name>``.  The default user
is ``swift``, which may not exist on your system.  These instructions are
intended to allow a developer to use his/her username for
``<your-user-name>:<your-group-name>``.

-----------------------
Installing dependencies
-----------------------

* On ``apt`` based systems::

        sudo apt-get update
        sudo apt-get install curl gcc memcached rsync sqlite3 xfsprogs \
                             git-core libffi-dev python-setuptools
        sudo apt-get install python-coverage python-dev python-nose \
                             python-simplejson python-xattr python-eventlet \
                             python-greenlet python-pastedeploy \
                             python-netifaces python-pip python-dnspython \
                             python-mock

* On ``yum`` based systems::

        sudo yum update
        sudo yum install curl gcc memcached rsync sqlite xfsprogs git-core \
                         libffi-devel xinetd python-setuptools \
                         python-coverage python-devel python-nose \
                         python-simplejson pyxattr python-eventlet \
                         python-greenlet python-paste-deploy \
                         python-netifaces python-pip python-dns \
                         python-mock

  This installs necessary system dependencies; and *most* of the python
  dependencies.  Later in the process setuptools/distribute or pip will
  install and/or upgrade some other stuff - it's getting harder to avoid.
  You can also install anything else you want, like screen, ssh, vim, etc.

Next, choose either :ref:`partition-section` or :ref:`loopback-section`.

.. _partition-section:

Using a partition for storage
=============================

If you are going to use a separate partition for Swift data, be sure to add
another device when creating the VM, and follow these instructions:

  #. Set up a single partition::

        sudo fdisk /dev/sdb
        sudo mkfs.xfs /dev/sdb1

  #. Edit ``/etc/fstab`` and add::

        /dev/sdb1 /mnt/sdb1 xfs noatime,nodiratime,nobarrier,logbufs=8 0 0

  #. Create the mount point and the individualized links::

        sudo mkdir /mnt/sdb1
        sudo mount /mnt/sdb1
        sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
        sudo chown ${USER}:${USER} /mnt/sdb1/*
        sudo mkdir /srv
        for x in {1..4}; do sudo ln -s /mnt/sdb1/$x /srv/$x; done
        sudo mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5 \
                      /srv/2/node/sdb2 /srv/2/node/sdb6 \
                      /srv/3/node/sdb3 /srv/3/node/sdb7 \
                      /srv/4/node/sdb4 /srv/4/node/sdb8 \
                      /var/run/swift
        sudo chown -R ${USER}:${USER} /var/run/swift
        # **Make sure to include the trailing slash after /srv/$x/**
        for x in {1..4}; do sudo chown -R ${USER}:${USER} /srv/$x/; done

  #. Next, skip to :ref:`common-dev-section`.


.. _loopback-section:

Using a loopback device for storage
===================================

If you want to use a loopback device instead of another partition, follow
these instructions:

  #. Create the file for the loopback device::

        sudo mkdir /srv
        sudo truncate -s 1GB /srv/swift-disk
        sudo mkfs.xfs /srv/swift-disk

     Modify size specified in the ``truncate`` command to make a larger or
     smaller partition as needed.

  #. Edit `/etc/fstab` and add::

        /srv/swift-disk /mnt/sdb1 xfs loop,noatime,nodiratime,nobarrier,logbufs=8 0 0

  #. Create the mount point and the individualized links::

        sudo mkdir /mnt/sdb1
        sudo mount /mnt/sdb1
        sudo mkdir /mnt/sdb1/1 /mnt/sdb1/2 /mnt/sdb1/3 /mnt/sdb1/4
        sudo chown ${USER}:${USER} /mnt/sdb1/*
        for x in {1..4}; do sudo ln -s /mnt/sdb1/$x /srv/$x; done
        sudo mkdir -p /srv/1/node/sdb1 /srv/1/node/sdb5 \
                      /srv/2/node/sdb2 /srv/2/node/sdb6 \
                      /srv/3/node/sdb3 /srv/3/node/sdb7 \
                      /srv/4/node/sdb4 /srv/4/node/sdb8 \
                      /var/run/swift
        sudo chown -R ${USER}:${USER} /var/run/swift
        # **Make sure to include the trailing slash after /srv/$x/**
        for x in {1..4}; do sudo chown -R ${USER}:${USER} /srv/$x/; done


.. _common-dev-section:

Common Post-Device Setup
========================

Add the following lines to ``/etc/rc.local`` (before the ``exit 0``)::

        mkdir -p /var/cache/swift /var/cache/swift2 /var/cache/swift3 /var/cache/swift4
        chown <your-user-name>:<your-group-name> /var/cache/swift*
        mkdir -p /var/run/swift
        chown <your-user-name>:<your-group-name> /var/run/swift

Note that on some systems you might have to create ``/etc/rc.local``.

On Fedora 19 or later, you need to place these in ``/etc/rc.d/rc.local``.

----------------
Getting the code
----------------

  #. Check out the python-swiftclient repo::

        cd $HOME; git clone https://github.com/openstack/python-swiftclient.git

  #. Build a development installation of python-swiftclient::

        cd $HOME/python-swiftclient; sudo python setup.py develop; cd -

     Ubuntu 12.04 users need to install python-swiftclient's dependencies before the installation of
     python-swiftclient. This is due to a bug in an older version of setup tools::

        cd $HOME/python-swiftclient; sudo pip install -r requirements.txt; sudo python setup.py develop; cd -

  #. Check out the swift repo::

        git clone https://github.com/openstack/swift.git

  #. Build a development installation of swift::

        cd $HOME/swift; sudo python setup.py develop; cd -

     Fedora 19 or later users might have to perform the following if development
     installation of swift fails::

        sudo pip install -U xattr

  #. Install swift's test dependencies::

        sudo pip install -r swift/test-requirements.txt

----------------
Setting up rsync
----------------

  #. Create ``/etc/rsyncd.conf``::

        sudo cp $HOME/swift/doc/saio/rsyncd.conf /etc/
        sudo sed -i "s/<your-user-name>/${USER}/" /etc/rsyncd.conf

     Here is the default ``rsyncd.conf`` file contents maintained in the repo
     that is copied and fixed up above:

     .. literalinclude:: /../saio/rsyncd.conf

  #. On Ubuntu, edit the following line in ``/etc/default/rsync``::

        RSYNC_ENABLE=true

     On Fedora, edit the following line in ``/etc/xinetd.d/rsync``::

        disable = no

     One might have to create the above files to perform the edits.

  #. On platforms with SELinux in ``Enforcing`` mode, either set to ``Permissive``::

        sudo setenforce Permissive

     Or just allow rsync full access::

        sudo setsebool -P rsync_full_access 1

  #. Start the rsync daemon

     * On Ubuntu, run::

        sudo service rsync restart

     * On Fedora, run::

        sudo systemctl restart xinetd.service
        sudo systemctl enable rsyncd.service
        sudo systemctl start rsyncd.service

     * On other xinetd based systems simply run::

        sudo service xinetd restart

  #. Verify rsync is accepting connections for all servers::

        rsync rsync://pub@localhost/

     You should see the following output from the above command::

        account6012
        account6022
        account6032
        account6042
        container6011
        container6021
        container6031
        container6041
        object6010
        object6020
        object6030
        object6040

------------------
Starting memcached
------------------

On non-Ubuntu distros you need to ensure memcached is running::

        sudo service memcached start
        sudo chkconfig memcached on

or::

        sudo systemctl enable memcached.service
        sudo systemctl start memcached.service

The tempauth middleware stores tokens in memcached. If memcached is not
running, tokens cannot be validated, and accessing Swift becomes impossible.

---------------------------------------------------
Optional: Setting up rsyslog for individual logging
---------------------------------------------------

  #. Install the swift rsyslogd configuration::

        sudo cp $HOME/swift/doc/saio/rsyslog.d/10-swift.conf /etc/rsyslog.d/

     Be sure to review that conf file to determine if you want all the logs
     in one file vs. all the logs separated out, and if you want hourly logs
     for stats processing. For convenience, we provide its default contents
     below:

     .. literalinclude:: /../saio/rsyslog.d/10-swift.conf

  #. Edit ``/etc/rsyslog.conf`` and make the following change (usually in the
     "GLOBAL DIRECTIVES" section)::

        $PrivDropToGroup adm

  #. If using hourly logs (see above) perform::

        sudo mkdir -p /var/log/swift/hourly

     Otherwise perform::

        sudo mkdir -p /var/log/swift

  #. Setup the logging directory and start syslog:

     * On Ubuntu::

        sudo chown -R syslog.adm /var/log/swift
        sudo chmod -R g+w /var/log/swift
        sudo service rsyslog restart

     * On Fedora::

        sudo chown -R root:adm /var/log/swift
        sudo chmod -R g+w /var/log/swift
        sudo systemctl restart rsyslog.service

---------------------
Configuring each node
---------------------

After performing the following steps, be sure to verify that Swift has access
to resulting configuration files (sample configuration files are provided with
all defaults in line-by-line comments).

  #. Optionally remove an existing swift directory::

        sudo rm -rf /etc/swift

  #. Populate the ``/etc/swift`` directory itself::

        cd $HOME/swift/doc; sudo cp -r saio/swift /etc/swift; cd -
        sudo chown -R ${USER}:${USER} /etc/swift

  #. Update ``<your-user-name>`` references in the Swift config files::

        find /etc/swift/ -name \*.conf | xargs sudo sed -i "s/<your-user-name>/${USER}/"

The contents of the configuration files provided by executing the above
commands are as follows:

  #. ``/etc/swift/swift.conf``

     .. literalinclude:: /../saio/swift/swift.conf

  #. ``/etc/swift/proxy-server.conf``

     .. literalinclude:: /../saio/swift/proxy-server.conf

  #. ``/etc/swift/object-expirer.conf``

     .. literalinclude:: /../saio/swift/object-expirer.conf

  #. ``/etc/swift/container-reconciler.conf``

     .. literalinclude:: /../saio/swift/container-reconciler.conf

  #. ``/etc/swift/account-server/1.conf``

     .. literalinclude:: /../saio/swift/account-server/1.conf

  #. ``/etc/swift/container-server/1.conf``

     .. literalinclude:: /../saio/swift/container-server/1.conf

  #. ``/etc/swift/object-server/1.conf``

     .. literalinclude:: /../saio/swift/object-server/1.conf

  #. ``/etc/swift/account-server/2.conf``

     .. literalinclude:: /../saio/swift/account-server/2.conf

  #. ``/etc/swift/container-server/2.conf``

     .. literalinclude:: /../saio/swift/container-server/2.conf

  #. ``/etc/swift/object-server/2.conf``

     .. literalinclude:: /../saio/swift/object-server/2.conf

  #. ``/etc/swift/account-server/3.conf``

     .. literalinclude:: /../saio/swift/account-server/3.conf

  #. ``/etc/swift/container-server/3.conf``

     .. literalinclude:: /../saio/swift/container-server/3.conf

  #. ``/etc/swift/object-server/3.conf``

     .. literalinclude:: /../saio/swift/object-server/3.conf

  #. ``/etc/swift/account-server/4.conf``

     .. literalinclude:: /../saio/swift/account-server/4.conf

  #. ``/etc/swift/container-server/4.conf``

     .. literalinclude:: /../saio/swift/container-server/4.conf

  #. ``/etc/swift/object-server/4.conf``

     .. literalinclude:: /../saio/swift/object-server/4.conf

.. _setup_scripts:

------------------------------------
Setting up scripts for running Swift
------------------------------------

  #. Copy the SAIO scripts for resetting the environment::

        cd $HOME/swift/doc; cp saio/bin/* $HOME/bin; cd -
        chmod +x $HOME/bin/*

  #. Edit the ``$HOME/bin/resetswift`` script

     The template ``resetswift`` script looks like the following:

        .. literalinclude:: /../saio/bin/resetswift

     If you are using a loopback device add an environment var to
     subsitute ``/dev/sdb1`` with ``/srv/swift-disk``::

        echo "export SAIO_BLOCK_DEVICE=/srv/swift-disk" >> $HOME/.bashrc

     If you did not set up rsyslog for individual logging, remove the ``find
     /var/log/swift...`` line::

        sed -i "/find \/var\/log\/swift/d" $HOME/bin/resetswift

     On Fedora, replace ``service <name> restart`` with ``systemctl restart
     <name>.service``::

        sed -i "s/service \(.*\) restart/systemctl restart \1.service/" $HOME/bin/resetswift


  #. Install the sample configuration file for running tests::

        cp $HOME/swift/test/sample.conf /etc/swift/test.conf

     The template ``test.conf`` looks like the following:

        .. literalinclude:: /../../test/sample.conf

  #. Add an environment variable for running tests below::

        echo "export SWIFT_TEST_CONFIG_FILE=/etc/swift/test.conf" >> $HOME/.bashrc

  #. Be sure that your ``PATH`` includes the ``bin`` directory::

        echo "export PATH=${PATH}:$HOME/bin" >> $HOME/.bashrc

  #. Source the above environment variables into your current environment::

        . $HOME/.bashrc

  #. Construct the initial rings using the provided script::

        remakerings

     The ``remakerings`` script looks like the following:

        .. literalinclude:: /../saio/bin/remakerings

     You can expect the output from this command to produce the following.  Note
     that 3 object rings are created in order to test storage policies and EC in
     the SAIO environment.  The EC ring is the only one with all 8 devices.
     There are also two replication rings, one for 3x replication and another
     for 2x replication, but those rings only use 4 devices::

        Device d0r1z1-127.0.0.1:6010R127.0.0.1:6010/sdb1_"" with 1.0 weight got id 0
        Device d1r1z2-127.0.0.1:6020R127.0.0.1:6020/sdb2_"" with 1.0 weight got id 1
        Device d2r1z3-127.0.0.1:6030R127.0.0.1:6030/sdb3_"" with 1.0 weight got id 2
        Device d3r1z4-127.0.0.1:6040R127.0.0.1:6040/sdb4_"" with 1.0 weight got id 3
        Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
        Device d0r1z1-127.0.0.1:6010R127.0.0.1:6010/sdb1_"" with 1.0 weight got id 0
        Device d1r1z2-127.0.0.1:6020R127.0.0.1:6020/sdb2_"" with 1.0 weight got id 1
        Device d2r1z3-127.0.0.1:6030R127.0.0.1:6030/sdb3_"" with 1.0 weight got id 2
        Device d3r1z4-127.0.0.1:6040R127.0.0.1:6040/sdb4_"" with 1.0 weight got id 3
        Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
        Device d0r1z1-127.0.0.1:6010R127.0.0.1:6010/sdb1_"" with 1.0 weight got id 0
        Device d1r1z1-127.0.0.1:6010R127.0.0.1:6010/sdb5_"" with 1.0 weight got id 1
        Device d2r1z2-127.0.0.1:6020R127.0.0.1:6020/sdb2_"" with 1.0 weight got id 2
        Device d3r1z2-127.0.0.1:6020R127.0.0.1:6020/sdb6_"" with 1.0 weight got id 3
        Device d4r1z3-127.0.0.1:6030R127.0.0.1:6030/sdb3_"" with 1.0 weight got id 4
        Device d5r1z3-127.0.0.1:6030R127.0.0.1:6030/sdb7_"" with 1.0 weight got id 5
        Device d6r1z4-127.0.0.1:6040R127.0.0.1:6040/sdb4_"" with 1.0 weight got id 6
        Device d7r1z4-127.0.0.1:6040R127.0.0.1:6040/sdb8_"" with 1.0 weight got id 7
        Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
        Device d0r1z1-127.0.0.1:6011R127.0.0.1:6011/sdb1_"" with 1.0 weight got id 0
        Device d1r1z2-127.0.0.1:6021R127.0.0.1:6021/sdb2_"" with 1.0 weight got id 1
        Device d2r1z3-127.0.0.1:6031R127.0.0.1:6031/sdb3_"" with 1.0 weight got id 2
        Device d3r1z4-127.0.0.1:6041R127.0.0.1:6041/sdb4_"" with 1.0 weight got id 3
        Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00
        Device d0r1z1-127.0.0.1:6012R127.0.0.1:6012/sdb1_"" with 1.0 weight got id 0
        Device d1r1z2-127.0.0.1:6022R127.0.0.1:6022/sdb2_"" with 1.0 weight got id 1
        Device d2r1z3-127.0.0.1:6032R127.0.0.1:6032/sdb3_"" with 1.0 weight got id 2
        Device d3r1z4-127.0.0.1:6042R127.0.0.1:6042/sdb4_"" with 1.0 weight got id 3
        Reassigned 1024 (100.00%) partitions. Balance is now 0.00.  Dispersion is now 0.00

  #. Read more about Storage Policies and your SAIO :doc:`policies_saio`

  #. Verify the unit tests run::

        $HOME/swift/.unittests

     Note that the unit tests do not require any swift daemons running.

  #. Start the "main" Swift daemon processes (proxy, account, container, and
     object)::

        startmain

     (The "``Unable to increase file descriptor limit.  Running as non-root?``"
     warnings are expected and ok.)

     The ``startmain`` script looks like the following:

        .. literalinclude:: /../saio/bin/startmain

  #. Get an ``X-Storage-Url`` and ``X-Auth-Token``::

        curl -v -H 'X-Storage-User: test:tester' -H 'X-Storage-Pass: testing' http://127.0.0.1:8080/auth/v1.0

  #. Check that you can ``GET`` account::

        curl -v -H 'X-Auth-Token: <token-from-x-auth-token-above>' <url-from-x-storage-url-above>

  #. Check that ``swift`` command provided by the python-swiftclient package works::

        swift -A http://127.0.0.1:8080/auth/v1.0 -U test:tester -K testing stat

  #. Verify the functional tests run::

        $HOME/swift/.functests

     (Note: functional tests will first delete everything in the configured
     accounts.)

  #. Verify the probe tests run::

        $HOME/swift/.probetests

     (Note: probe tests will reset your environment as they call ``resetswift``
     for each test.)

----------------
Debugging Issues
----------------

If all doesn't go as planned, and tests fail, or you can't auth, or something
doesn't work, here are some good starting places to look for issues:

#. Everything is logged using system facilities -- usually in ``/var/log/syslog``,
   but possibly in ``/var/log/messages`` on e.g. Fedora -- so that is a good first
   place to look for errors (most likely python tracebacks).
#. Make sure all of the server processes are running.  For the base
   functionality, the Proxy, Account, Container, and Object servers
   should be running.
#. If one of the servers are not running, and no errors are logged to syslog,
   it may be useful to try to start the server manually, for example:
   ``swift-object-server /etc/swift/object-server/1.conf`` will start the
   object server.  If there are problems not showing up in syslog,
   then you will likely see the traceback on startup.
#. If you need to, you can turn off syslog for unit tests. This can be
   useful for environments where ``/dev/log`` is unavailable, or which
   cannot rate limit (unit tests generate a lot of logs very quickly).
   Open the file ``SWIFT_TEST_CONFIG_FILE`` points to, and change the
   value of ``fake_syslog`` to ``True``.
