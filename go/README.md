Hummingbird
===========

Hummingbird is a Golang implementation of some parts of [OpenStack Swift][1].
The idea is to keep the same protocols and on-disk layout while dramatically
improving performance.


Completeness
------------

The object-server, object-replicator and object-auditor are considered
feature-complete and testing is ongoing. The proxy server is currently only
complete enough to run simple GET, PUT, and DELETE benchmarks.


Prerequisites: SAIO
-------------------

First, you should have a working [SAIO][2] with only replication type storage
policies configured in `/etc/swift.conf`.

You will also need to configure your syslog to listen for UDP packets. If
you're using rsyslog or a standard SAIO, simply uncomment these two lines in
/etc/rsyslog.conf:

    # provides UDP syslog reception
    $ModLoad imudp
    $UDPServerRun 514

Save it, then run `sudo service rsyslog restart`.


Prerequisites: Go
-----------------

Next, you must have a working Go development environment. While these steps
assume a SAIO, the paths can be tweaked to meet your needs in other
environments later.

1. [Download Go binaries for your platform][3].
1. Pick a path to install Go binaries, and extract them. The recommended
   location is `/usr/local`:

        tar -C /usr/local -zxf go1.6.2.linux-amd64.tar.gz

1. Now, export three environment variables to inform both Go and your shell
   where to find what you'll need to run both Go and Hummingbird binaries.

   * `GOROOT` -- The `go` subdirectory of the location you extracted to above,
     for instance, `/usr/local/go`. This is more where the core binaries and
     libraries for Go are kept -- not any code you write.

   * `GOPATH` -- The folder where you intend to keep Go code, binaries, etc.
     For simplicity, we'll do this in your home directory, so we'll use
     `~/go`.

   * `PATH` -- Append the bin dirs within both of the above onto your PATH,
     with your normal path first, then $GOROOT/bin, and your $GOPATH/bin:

          export GOROOT=/usr/local/go
          export GOPATH=~/go
          export PATH=$PATH:$GOROOT/bin:$GOPATH/bin

1. It's also recommended to add these three lines either to the end of the
   `/etc/profile` on the system, or at least to your own user's profile.

1. Ensure you can now use the new `go` binary:

        $ go version
        go version go1.6.2 linux/amd64


Installing Hummingbird
----------------------

Now we need to bring the Hummingbird codebase into our GOPATH. We already have
Swift cloned in our SAIO, so it's easier for us to just symlink it into a
folder structure within our GOPATH:

    mkdir -p $GOPATH/src/github.com/openstack
    ln -s ~/swift $GOPATH/src/github.com/openstack/swift

Since Hummingbird work is ongoing in a feature branch, be sure you checkout the
appropriate branch before attempting to install:

    cd $GOPATH/src/github.com/openstack/swift
    git checkout feature/hummingbird

Now you're ready to compile, test, and install Hummingbird. A Makefile is
provided to make this simpler:

    cd $GOPATH/src/github.com/openstack/swift/go
    make get test all

Once complete, your output will look similar like this:

    go get -t ./...
    go vet ./...
    go test -cover ./...
    ?       github.com/openstack/swift/go/bench [no test files]
    ?       github.com/openstack/swift/go/client    [no test files]
    ?       github.com/openstack/swift/go/cmd   [no test files]
    ok      github.com/openstack/swift/go/hummingbird   4.344s  coverage: 62.1% of statements
    ok      github.com/openstack/swift/go/middleware    0.017s  coverage: 82.7% of statements
    ok      github.com/openstack/swift/go/objectserver  3.391s  coverage: 71.0% of statements
    ok      github.com/openstack/swift/go/probe 3.126s  coverage: 88.7% of statements
    ?       github.com/openstack/swift/go/proxyserver   [no test files]
    go build -o bin/hummingbird -ldflags "-X main.Version=2.6.0-344-g12276d7" cmd/hummingbird.go

To install the hummingbird executable to /usr/bin, run this last command:

    $ sudo GOPATH=$GOPATH GOROOT=$GOROOT PATH=$PATH:$GOROOT/bin make develop


Configuration
-------------

If you'd like to start Hummingbird processes as a user other than root (like
the swift user), you'll need to create `/var/run/hummingbird` with proper
permissions. It will store pid files in this directory.

    mkdir -p /var/run/hummingbird
    chown -R swift:swift /var/run/hummingbird

Other than that, Hummingbird will continue using the same configuration files
you already use for Swift processes today, and some `hummingbird` commands can
be provided a specific configuration file to use.


Running
-------

Now that you've successfully installed Hummingbird, you can run the standard
functional tests to validate things are working. As an example, if you want to
run standard Swift proxy, account, and container services, but test out
Hummingbird's object server, first stop the Swift object-server processes
before starting Hummingbird's:

    swift-init object-server stop
    hummingbird start object

The `hummingbird` command handles starting services, managing pid files, etc.,
similar to `swift-init`:

    hummingbird <start|reload|restart|shutdown|stop> <all|object|proxy|object-replicator|object-auditor>

You may also run daemons interactively: 

    hummingbird object [-c /etc/swift/object-server.conf]


Caveats
-------
Hummingbird components that are run interactively will continue to log only to
syslog, so check your normal logs if you're not seeing any output when running
in this manner.

If you see a small handful of unexpected test failures in a mixed
Swift/Hummingbird environment, check to make sure that the version of Swift
you're running isn't too far off from the current version upon which the
feature/hummingbird branch is currently based (i.e., a Swift 2.4.0 tag versus
Hummingbird from a feature branch based on 2.6.0+). You can check this by
running `git describe --tags` in your ~/swift directory where you installed
both Swift and Hummingbird.



   [1]: http://swift.openstack.org/
   [2]: http://docs.openstack.org/developer/swift/development_saio.html
   [3]: https://golang.org/doc/install
