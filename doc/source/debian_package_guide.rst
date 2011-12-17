=======================================================
Building and Deploying Custom Debian packages for Swift
=======================================================

This documents setting up the prerequisites, downloading the code and building
Debian packages necessary to deploy the various components of the swift 
project code.  These steps were done on a server running 
*Ubuntu 10.04 LTS (Lucid Lynx)*, but should give a good idea what to do on 
other distros.

------------------------------------------
Instructions for Deploying Swift-Core PPAs
------------------------------------------

These packages are built from the current development branch "trunk" 

* Add swift-core/ppa repository. As root:::

       apt-get install python-software-properties
       add-apt-repository ppa:swift-core/trunk
       apt-get update

* Install the swift base packages::

       apt-get install python-swift
       apt-get install swift

* Install the swift packages depending on your implementations::

       apt-get install swift-auth
       apt-get install swift-proxy
       apt-get install swift-account
       apt-get install swift-container
       apt-get install swift-object

* Copy sample configuration files to `/etc/swift` directory 
  and rename them to `*.conf files`::
     
       cp -a /usr/share/doc/swift/*.conf-sample /etc/swift/ 
       cd /etc/swift ; rename 's/\-sample$//' *.conf-sample

* For servers running the swift-account, swift-container or 
  swift-object the rsync.conf file should be moved to 
  the `/etc` directory::

       cd /etc/swift
       mv rsyncd.conf /etc

* Modify configuration files to meet your implementation requirements
  the defaults have been not been geared to a multi-server implementation.

---------------------------------------------------
Instructions for Building Debian Packages for Swift
---------------------------------------------------

* Add swift-core/ppa repository and install prerequisites. As root::

       apt-get install python-software-properties
       add-apt-repository ppa:swift-core/release
       apt-get update
       apt-get install curl gcc bzr python-configobj python-coverage python-dev python-nose python-setuptools python-simplejson python-xattr python-webob python-eventlet python-greenlet debhelper python-sphinx python-all python-openssl python-pastedeploy python-netifaces bzr-builddeb

* As you

  #. Tell bzr who you are::

       bzr whoami '<Your Name> <youremail@.example.com>'
       bzr lp-login <your launchpad id>

  #. Create a local bazaar repository for dev/testing:: 

       bzr init-repo swift

  #. Pull down the swift/debian files::

       cd swift 
       bzr branch lp:~swift-core/swift/debian

  #. If you want to merge in a branch::
     
       cd debian
       bzr merge lp:<path-to-branch>
  
  #. Create the debian packages:: 
  
       cd debian 
       bzr bd --builder='debuild -uc -us'
 
  #. Upload packages to your target servers::
 
       cd .. 
       scp *.deb root@<swift-target-server>:~/.


----------------------------------------------------
Instructions for Deploying Debian Packages for Swift
----------------------------------------------------

* On a Target Server, As root:

  #. Setup the swift ppa::
 
       add-apt-repository ppa:swift-core/release
       apt-get update

  #. Install dependencies::
 
       apt-get install rsync python-openssl python-setuptools python-webob
       python-simplejson python-xattr python-greenlet python-eventlet
       python-netifaces

  #. Install base packages::

       dpkg -i python-swift_<version>_all.deb 
       dpkg -i swift_<version>_all.deb

  #. Install packages depending on your implementation::

       dpkg -i swift-auth_<version>_all.deb    
       dpkg -i swift-proxy_<version>_all.deb
       dpkg -i swift-account_<version>_all.deb  
       dpkg -i swift-container_<version>_all.deb  
       dpkg -i swift-object_<version>_all.deb  
       dpkg -i swift-doc_<version>_all.deb

  #. Copy sample configuration files to `/etc/swift` directory 
     and rename them to `*.conf files`::

       cp -a /usr/share/doc/swift/*.conf-sample /etc/swift/ 
       cd /etc/swift 
       rename 's/\-sample$//' *.conf-sample

  #. For servers running the swift-account, swift-container or 
     swift-object the rsync.conf file should be moved to 
     the `/etc` directory::

       cd /etc/swift/ 
       mv rsyncd.conf /etc

  #. Modify configuration files to meet your implementation requirements
     the defaults have been not been geared to a multi-server implementation.
