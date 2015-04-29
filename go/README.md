Hummingbird
=========

Hummingbird is a golang implementation of some parts of [Openstack Swift](http://swift.openstack.org/).  The idea is to keep the same protocols and on-disk layout, but improve performance dramatically.


Completeness
------------

The object server, replicator and auditor are considered feature complete and testing is ongoing.  The proxy server is currently only complete enough to run simple GET, PUT, and DELETE benchmarks.


Installation
--------------

First, you should have a working [SAIO](http://docs.openstack.org/developer/swift/development_saio.html). (With no storage policies)

You will also need to configure your syslog to listen for UDP packets:

For a SAIO, change these lines in /etc/rsyslog.conf:
```
# provides UDP syslog reception
$ModLoad imudp
$UDPServerRun 514
```

and then sudo service rsyslog restart.

Next, you must have a working [Go development environment](https://golang.org/doc/install).

Better build instructions coming...  For now, do something like on your SAIO:

```sh
mkdir -p $GOPATH/src/github.com/openstack
ln -s ~/swift $GOPATH/src/github.com/openstack/swift
cd $GOPATH/src/github.com/openstack/swift/go
make get test all
```


Running
-------

The "hummingbird" executable handles starting services, reading and writing pid files, etc., similar to swift-init.

```sh
hummingbird [start/stop/restart] [object/proxy/all]
```

If you'd like to start it from a user other than root, you'll probably need to create /var/run/hummingbird with the correct permissions.
