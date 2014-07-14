If you would like to contribute to the development of OpenStack,
you must follow the steps in the "If you're a developer"
section of this page: [http://wiki.openstack.org/HowToContribute](http://wiki.openstack.org/HowToContribute#If_you.27re_a_developer)

Once those steps have been completed, changes to OpenStack
should be submitted for review via the Gerrit tool, following
the workflow documented at [http://wiki.openstack.org/GerritWorkflow](http://wiki.openstack.org/GerritWorkflow).

Gerrit is the review system used in the OpenStack projects.  We're sorry, but
we won't be able to respond to pull requests submitted through GitHub.

Bugs should be filed [on Launchpad](https://bugs.launchpad.net/swift),
not in GitHub's issue tracker.

Recommended workflow
====================

 * Set up a [Swift All-In-One VM](http://docs.openstack.org/developer/swift/development_saio.html).

 * Make your changes.

 * Run unit tests, functional tests, probe tests
   ``./.unittests``
   ``./.functests``
   ``./.probetests``

 * Run ``tox`` (no command-line args needed)

 * ``git review``
