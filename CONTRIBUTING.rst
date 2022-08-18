Contributing to OpenStack Swift
===============================

Who is a Contributor?
---------------------

Put simply, if you improve Swift, you're a contributor. The easiest way to
improve the project is to tell us where there's a bug. In other words, filing
a bug is a valuable and helpful way to contribute to the project.

Once a bug has been filed, someone will work on writing a patch to fix the
bug. Perhaps you'd like to fix a bug. Writing code to fix a bug or add new
functionality is tremendously important.

Once code has been written, it is submitted upstream for review. All code,
even that written by the most senior members of the community, must pass code
review and all tests before it can be included in the project. Reviewing
proposed patches is a very helpful way to be a contributor.

Swift is nothing without the community behind it. We'd love to welcome you to
our community. Come find us in #openstack-swift on OFTC IRC or on the
OpenStack dev mailing list.

For general information on contributing to OpenStack, please check out the
`contributor guide <https://docs.openstack.org/contributors/>`_ to get started.
It covers all the basics that are common to all OpenStack projects: the accounts
you need, the basics of interacting with our Gerrit review system, how we
communicate as a community, etc.

If you want more Swift related project documentation make sure you checkout
the Swift developer (contributor) documentation at
https://docs.openstack.org/swift/latest/

Filing a Bug
~~~~~~~~~~~~

Filing a bug is the easiest way to contribute. We use Launchpad as a bug
tracker; you can find currently-tracked bugs at
https://bugs.launchpad.net/swift.
Use the `Report a bug <https://bugs.launchpad.net/swift/+filebug>`__ link to
file a new bug.

If you find something in Swift that doesn't match the documentation or doesn't
meet your expectations with how it should work, please let us know. Of course,
if you ever get an error (like a Traceback message in the logs), we definitely
want to know about that. We'll do our best to diagnose any problem and patch
it as soon as possible.

A bug report, at minimum, should describe what you were doing that caused the
bug. "Swift broke, pls fix" is not helpful. Instead, something like "When I
restarted syslog, Swift started logging traceback messages" is very helpful.
The goal is that we can reproduce the bug and isolate the issue in order to
apply a fix. If you don't have full details, that's ok. Anything you can
provide is helpful.

You may have noticed that there are many tracked bugs, but not all of them
have been confirmed. If you take a look at an old bug report and you can
reproduce the issue described, please leave a comment on the bug about that.
It lets us all know that the bug is very likely to be valid.

Reviewing Someone Else's Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

All code reviews in OpenStack projects are done on
https://review.opendev.org/. Reviewing patches is one of the most effective
ways you can contribute to the community.

We've written REVIEW_GUIDELINES.rst (found in this source tree) to help you
give good reviews.

https://wiki.openstack.org/wiki/Swift/PriorityReviews is a starting point to
find what reviews are priority in the community.

What do I work on?
------------------

If you're looking for a way to write and contribute code, but you're not sure
what to work on, check out the "wishlist" bugs in the bug tracker. These are
normally smaller items that someone took the time to write down but didn't
have time to implement.

And please join #openstack-swift on OFTC IRC to tell us what you're working on.

Getting Started
---------------

https://docs.openstack.org/swift/latest/first_contribution_swift.html

Once those steps have been completed, changes to OpenStack
should be submitted for review via the Gerrit tool, following
the workflow documented at
http://docs.openstack.org/infra/manual/developers.html#development-workflow.

Gerrit is the review system used in the OpenStack projects. We're sorry, but
we won't be able to respond to pull requests submitted through GitHub.

Bugs should be filed `on Launchpad <https://bugs.launchpad.net/swift>`__,
not in GitHub's issue tracker.

Swift Design Principles
=======================

-  `The Zen of Python <http://legacy.python.org/dev/peps/pep-0020/>`__
-  Simple Scales
-  Minimal dependencies
-  Re-use existing tools and libraries when reasonable
-  Leverage the economies of scale
-  Small, loosely coupled RESTful services
-  No single points of failure
-  Start with the use case
-  ... then design from the cluster operator up
-  If you haven't argued about it, you don't have the right answer yet
   :)
-  If it is your first implementation, you probably aren't done yet :)

Please don't feel offended by difference of opinion. Be prepared to
advocate for your change and iterate on it based on feedback. Reach out
to other people working on the project on
`IRC <http://eavesdrop.openstack.org/irclogs/%23openstack-swift/>`__ or
the `mailing
list <http://lists.openstack.org/pipermail/openstack-discuss/>`__ - we want
to help.

Recommended workflow
====================

-  Set up a `Swift All-In-One
   VM <https://docs.openstack.org/swift/latest/development_saio.html>`__\ (SAIO).

-  Make your changes. Docs and tests for your patch must land before or
   with your patch.

-  Run unit tests, functional tests, probe tests ``./.unittests``
   ``./.functests`` ``./.probetests``

-  Run ``tox`` (no command-line args needed)

-  ``git review``

Notes on Testing
================

Running the tests above against Swift in your development environment
(ie your SAIO) will catch most issues. Any patch you propose is expected
to be both tested and documented and all tests should pass.

If you want to run just a subset of the tests while you are developing,
you can use pytest:

.. code-block:: console

    cd test/unit/common/middleware/ && pytest test_healthcheck.py

To check which parts of your code are being exercised by a test, you can
run tox and then point your browser to swift/cover/index.html:

.. code-block:: console

    tox -e py3 -- test.unit.common.middleware.test_healthcheck:TestHealthCheck.test_healthcheck

Swift's unit tests are designed to test small parts of the code in
isolation. The functional tests validate that the entire system is
working from an external perspective (they are "black-box" tests). You
can even run functional tests against public Swift endpoints. The
probetests are designed to test much of Swift's internal processes. For
example, a test may write data, intentionally corrupt it, and then
ensure that the correct processes detect and repair it.

When your patch is submitted for code review, it will automatically be
tested on the OpenStack CI infrastructure. In addition to many of the
tests above, it will also be tested by several other OpenStack test
jobs.

Once your patch has been reviewed and approved by core reviewers and
has passed all automated tests, it will be merged into the Swift source
tree.

Ideas
=====

https://wiki.openstack.org/wiki/Swift/ideas

If you're working on something, it's a very good idea to write down
what you're thinking about. This lets others get up to speed, helps
you collaborate, and serves as a great record for future reference.
Write down your thoughts somewhere and put a link to it here. It
doesn't matter what form your thoughts are in; use whatever is best
for you. Your document should include why your idea is needed and your
thoughts on particular design choices and tradeoffs. Please include
some contact information (ideally, your IRC nick) so that people can
collaborate with you.
