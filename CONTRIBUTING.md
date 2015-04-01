If you would like to contribute to the development of OpenStack,
you must follow the steps in this page: [http://docs.openstack.org/infra/manual/developers.html](http://docs.openstack.org/infra/manual/developers.html)

Once those steps have been completed, changes to OpenStack
should be submitted for review via the Gerrit tool, following
the workflow documented at [http://docs.openstack.org/infra/manual/developers.html#development-workflow](http://docs.openstack.org/infra/manual/developers.html#development-workflow).

Gerrit is the review system used in the OpenStack projects.  We're sorry, but
we won't be able to respond to pull requests submitted through GitHub.

Bugs should be filed [on Launchpad](https://bugs.launchpad.net/swift),
not in GitHub's issue tracker.


Swift Design Principles
=======================

  * [The Zen of Python](http://legacy.python.org/dev/peps/pep-0020/)
  * Simple Scales
  * Minimal dependencies
  * Re-use existing tools and libraries when reasonable
  * Leverage the economies of scale
  * Small, loosely coupled RESTful services
  * No single points of failure
  * Start with the use case
  * ... then design from the cluster operator up
  * If you haven't argued about it, you don't have the right answer yet :)
  * If it is your first implementation, you probably aren't done yet :)

Please don't feel offended by difference of opinion.  Be prepared to advocate
for your change and iterate on it based on feedback.  Reach out to other people
working on the project on
[IRC](http://eavesdrop.openstack.org/irclogs/%23openstack-swift/) or the
[mailing list](http://lists.openstack.org/pipermail/openstack-dev/) - we want
to help.

Recommended workflow
====================

 * Set up a [Swift All-In-One VM](http://docs.openstack.org/developer/swift/development_saio.html)(SAIO).

 * Make your changes. Docs and tests for your patch must land before
   or with your patch.

 * Run unit tests, functional tests, probe tests
   ``./.unittests``
   ``./.functests``
   ``./.probetests``

 * Run ``tox`` (no command-line args needed)

 * ``git review``

Notes on Testing
================

Running the tests above against Swift in your development environment (ie
your SAIO) will catch most issues. Any patch you propose is expected to be
both tested and documented and all tests should pass.

If you want to run just a subset of the tests while you are developing, you
can use nosetests::

    cd test/unit/common/middleware/ && nosetests test_healthcheck.py

To check which parts of your code are being exercised by a test, you can run
tox and then point your browser to swift/cover/index.html::

    tox -e py27 -- test.unit.common.middleware.test_healthcheck:TestHealthCheck.test_healthcheck

Swift's unit tests are designed to test small parts of the code in isolation.
The functional tests validate that the entire system is working from an
external perspective (they are "black-box" tests). You can even run functional
tests against public Swift endpoints. The probetests are designed to test much
of Swift's internal processes. For example, a test may write data,
intentionally corrupt it, and then ensure that the correct processes detect
and repair it.

When your patch is submitted for code review, it will automatically be tested
on the OpenStack CI infrastructure. In addition to many of the tests above, it
will also be tested by several other OpenStack test jobs.

Once your patch has been reviewed and approved by two core reviewers and has
passed all automated tests, it will be merged into the Swift source tree.

Specs
=====

The [``swift-specs``](https://github.com/openstack/swift-specs) repo
can be used for collaborative design work before a feature is implemented.

Openstack's gerrit system is used to collaborate on the design spec. Once 
approved Openstack provides a doc site to easily read these [specs](http://specs.openstack.org/openstack/swift-specs/)

A spec is needed for more impactful features. Coordinating a feature between
many devs (especially across companies) is a great example of when a spec is
needed. If you are unsure if a spec document is needed, please feel free to
ask in #openstack-swift on freenode IRC.
