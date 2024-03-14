Working with a feature branch
=============================

Creating a patch
----------------

To propose a patch to a feature branch, create a local branch based on the
feature branch, e.g.::

      git checkout -b branch_name remotes/origin/feature/mpu

When you have made your changes, push to gerrit as usual::

      git review


Merging master to the feature branch
------------------------------------

From time to time it is necessary to merge from master to the feature branch.
First, get master up-to-date::

      git checkout master
      git pull --ff-only origin master

Then create a local branch off the feature branch into which you will merge
the master branch, e.g.::

      git checkout -b merge-master remotes/origin/feature/mpu
      git merge remotes/origin/master

If everything merges cleanly then the tip of your local branch will be a commit
for the merge. However, there may be merge conflicts to resolve. Once you've
taken care of them, add all the affected files and then commit the merge::

      git add
      git commit -m 'merge master to feature/mpu'

.. note::
      The ``git commit`` is only necessary when there have been merge
      conflicts.

Push the merged branch to gerrit for review, using the ``-R`` option::

      git review -R
