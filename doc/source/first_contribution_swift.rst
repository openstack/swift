===========================
First Contribution to Swift
===========================

-------------
Getting Swift
-------------

.. highlight: none

Swift's source code is hosted on github and managed with git.  The current
trunk can be checked out like this::

    git clone https://github.com/openstack/swift.git

This will clone the Swift repository under your account.

A source tarball for the latest release of Swift is available on the
`launchpad project page <https://launchpad.net/swift>`_.

Prebuilt packages for Ubuntu and RHEL variants are available.

* `Swift Ubuntu Packages <https://launchpad.net/ubuntu/+source/swift>`_
* `Swift RDO Packages <https://www.rdoproject.org/Repositories>`_

--------------------
Source Control Setup
--------------------

Swift uses ``git`` for source control. The OpenStack
`Developer's Guide <http://docs.openstack.org/infra/manual/developers.html>`_
describes the steps for setting up Git and all the necessary accounts for
contributing code to Swift.

----------------
Changes to Swift
----------------

Once you have the source code and source control set up, you can make your
changes to Swift.

-------
Testing
-------

The :doc:`Development Guidelines <development_guidelines>` describe the testing
requirements before submitting Swift code.

In summary, you can execute tox from the swift home directory (where you
checked out the source code)::

    tox

Tox will present tests results. Notice that in the beginning, it is very common
to break many coding style guidelines.

--------------------------
Proposing changes to Swift
--------------------------

The OpenStack
`Developer's Guide <http://docs.openstack.org/infra/manual/developers.html>`_
describes the most common ``git`` commands that you will need.

Following is a list of the commands that you need to know for your first
contribution to Swift:

To clone a copy of Swift::

    git clone https://github.com/openstack/swift.git

Under the swift directory, set up the Gerrit repository. The following command
configures the repository to know about Gerrit and installs the ``Change-Id``
commit hook. You only need to do this once::

    git review -s

To create your development branch (substitute branch_name for a name of your
choice::

    git checkout -b <branch_name>

To check the files that have been updated in your branch::

    git status

To check the differences between your branch and the repository::

    git diff

Assuming you have not added new files, you commit all your changes using::

    git commit -a

Read the `Summary of Git commit message structure <https://wiki.openstack.org/wiki/GitCommitMessages?%22Summary%20of%20Git%20commit%20message%20structure%22#Summary_of_Git_commit_message_structure>`_
for best practices on writing the commit message. When you are ready to send
your changes for review use::

    git review

If successful, Git response message will contain a URL you can use to track your
changes.

If you need to make further changes to the same review, you can commit them
using::

    git commit -a --amend

This will commit the changes under the same set of changes you issued earlier.
Notice that in order to send your latest version for review, you will still
need to call::

    git review

---------------------
Tracking your changes
---------------------

After proposing changes to Swift, you can track them at
https://review.openstack.org. After logging in, you will see a dashboard of
"Outgoing reviews" for changes you have proposed, "Incoming reviews" for
changes you are reviewing, and "Recently closed" changes for which you were
either a reviewer or owner.

.. _post-rebase-instructions:

------------------------
Post rebase instructions
------------------------

After rebasing, the following steps should be performed to rebuild the swift
installation. Note that these commands should be performed from the root of the
swift repo directory (e.g. ``$HOME/swift/``)::

    sudo python setup.py develop
    sudo pip install -r test-requirements.txt

If using TOX, depending on the changes made during the rebase, you may need to
rebuild the TOX environment (generally this will be the case if
test-requirements.txt was updated such that a new version of a package is
required), this can be accomplished using the ``-r`` argument to the TOX cli::

    tox -r

You can include any of the other TOX arguments as well, for example, to run the
pep8 suite and rebuild the TOX environment the following can be used::

    tox -r -e pep8

The rebuild option only needs to be specified once for a particular build (e.g.
pep8), that is further invocations of the same build will not require this
until the next rebase.

---------------
Troubleshooting
---------------

You may run into the following errors when starting Swift if you rebase
your commit using::

    git rebase

.. code-block:: python

   Traceback (most recent call last):
       File "/usr/local/bin/swift-init", line 5, in <module>
           from pkg_resources import require
       File "/usr/lib/python2.7/dist-packages/pkg_resources.py", line 2749, in <module>
           working_set = WorkingSet._build_master()
       File "/usr/lib/python2.7/dist-packages/pkg_resources.py", line 446, in _build_master
           return cls._build_from_requirements(__requires__)
       File "/usr/lib/python2.7/dist-packages/pkg_resources.py", line 459, in _build_from_requirements
           dists = ws.resolve(reqs, Environment())
       File "/usr/lib/python2.7/dist-packages/pkg_resources.py", line 628, in resolve
           raise DistributionNotFound(req)
   pkg_resources.DistributionNotFound: swift==2.3.1.devXXX

(where XXX represents a dev version of Swift).

.. code-block:: python

   Traceback (most recent call last):
       File "/usr/local/bin/swift-proxy-server", line 10, in <module>
         execfile(__file__)
       File "/home/swift/swift/bin/swift-proxy-server", line 23, in <module>
         sys.exit(run_wsgi(conf_file, 'proxy-server', **options))
       File "/home/swift/swift/swift/common/wsgi.py", line 888, in run_wsgi
         loadapp(conf_path, global_conf=global_conf)
       File "/home/swift/swift/swift/common/wsgi.py", line 390, in loadapp
         func(PipelineWrapper(ctx))
       File "/home/swift/swift/swift/proxy/server.py", line 602, in modify_wsgi_pipeline
         ctx = pipe.create_filter(filter_name)
       File "/home/swift/swift/swift/common/wsgi.py", line 329, in create_filter
         global_conf=self.context.global_conf)
       File "/usr/lib/python2.7/dist-packages/paste/deploy/loadwsgi.py", line 296, in loadcontext
         global_conf=global_conf)
       File "/usr/lib/python2.7/dist-packages/paste/deploy/loadwsgi.py", line 328, in _loadegg
         return loader.get_context(object_type, name, global_conf)
       File "/usr/lib/python2.7/dist-packages/paste/deploy/loadwsgi.py", line 620, in get_context
         object_type, name=name)
       File "/usr/lib/python2.7/dist-packages/paste/deploy/loadwsgi.py", line 659, in find_egg_entry_point
         for prot in protocol_options] or '(no entry points)'))))
   LookupError: Entry point 'versioned_writes' not found in egg 'swift' (dir: /home/swift/swift; protocols: paste.filter_factory, paste.filter_app_factory; entry_points: )

This happens because ``git rebase`` will retrieve code for a different version
of Swift in the development stream, but the start scripts under
``/usr/local/bin`` have not been updated. The solution is to follow the steps
described in the :ref:`post-rebase-instructions` section.
