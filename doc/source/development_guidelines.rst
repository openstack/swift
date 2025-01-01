======================
Development Guidelines
======================

-----------------
Coding Guidelines
-----------------

For the most part we try to follow PEP 8 guidelines which can be viewed
here: http://www.python.org/dev/peps/pep-0008/

------------------
Testing Guidelines
------------------

Swift has a comprehensive suite of tests and pep8 checks that are run on all
submitted code, and it is recommended that developers execute the tests
themselves to catch regressions early.  Developers are also expected to keep
the test suite up-to-date with any submitted code changes.

Swift's tests and pep8 checks can be executed in an isolated environment
with ``tox``: http://tox.testrun.org/

To execute the tests:

* Ensure ``pip`` and ``virtualenv`` are upgraded to satisfy the version
  requirements listed in the OpenStack `global requirements`_::

    pip install pip -U
    pip install virtualenv -U

.. _`global requirements`: https://github.com/openstack/requirements/blob/master/global-requirements.txt

* Install ``tox``::

    pip install tox

* Generate list of  distribution packages to install for testing::

    tox -e bindep

  Now install these packages using your distribution package manager
  like apt-get, dnf, yum, or zypper.

* Run ``tox`` from the root of the swift repo::

    tox

To run a selected subset of unit tests with ``pytest``:

* Create a virtual environment with ``tox``::

    tox devenv -e py3 .env

.. note::
  Alternatively, here are the steps of manual preparation of the virtual environment::

    virtualenv .env
    source .env/bin/activate
    pip3 install -r requirements.txt -r test-requirements.txt -c py36-constraints.txt
    pip3 install -e .
    deactivate

* Activate the virtual environment::

    source .env/bin/activate

* Run some unit tests, for example::

    pytest test/unit/common/middleware/crypto

* Run all unit tests::

    pytest test/unit

.. note::
  If you installed using ``cd ~/swift; sudo python setup.py develop``, you may
  need to do ``cd ~/swift; sudo chown -R ${USER}:${USER} swift.egg-info`` prior
  to running ``tox``.

* By default ``tox`` will run **all of the unit test** and pep8 checks listed in
  the ``tox.ini`` file ``envlist`` option. A subset of the test environments
  can be specified on the ``tox`` command line or by setting the ``TOXENV``
  environment variable. For example, to run only the pep8 checks and python3
  unit tests use::

    tox -e pep8,py3

  or::

    TOXENV=py3,pep8 tox

  To run unit tests with python3.12 specifically::

    tox -e py312

.. note::
  As of ``tox`` version 2.0.0, most environment variables are not automatically
  passed to the test environment. Swift's ``tox.ini`` overrides this default
  behavior so that variable names matching ``SWIFT_*`` and ``*_proxy`` will be
  passed, but you may need to run ``tox --recreate`` for this to take effect
  after upgrading from ``tox`` <2.0.0.

  Conversely, if you do not want those environment variables to be passed to
  the test environment then you will need to unset them before calling ``tox``.

  Also, if you ever encounter DistributionNotFound, try to use ``tox
  --recreate`` or remove the ``.tox`` directory to force ``tox`` to recreate the
  dependency list.

  Swift's tests require having an XFS directory available in ``/tmp`` or
  in the ``TMPDIR`` environment variable.

Swift's functional tests may be executed against a :doc:`development_saio` or
other running Swift cluster using the command::

  tox -e func

The endpoint and authorization credentials to be used by functional tests
should be configured in the ``test.conf`` file as described in the section
:ref:`setup_scripts`.

The environment variable ``SWIFT_TEST_POLICY`` may be set to specify a
particular storage policy *name* that will be used for testing. When set, tests
that would otherwise not specify a policy or choose a random policy from
those available will instead use the policy specified. Tests that use more than
one policy will include the specified policy in the set of policies used. The
specified policy must be available on the cluster under test.

For example, this command would run the functional tests using policy
'silver'::

  SWIFT_TEST_POLICY=silver tox -e func

To run a single functional test, use the ``--no-discover`` option together with
a path to a specific test method, for example::

  tox -e func -- --no-discover test.functional.tests.TestFile.testCopy


In-process functional testing
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If the ``test.conf`` file is not found then the functional test framework will
instantiate a set of Swift servers in the same process that executes the
functional tests. This 'in-process test' mode may also be enabled (or disabled)
by setting the environment variable ``SWIFT_TEST_IN_PROCESS`` to a true (or
false) value prior to executing ``tox -e func``.

When using the 'in-process test' mode some server configuration options may be
set using environment variables:

- the optional in-memory object server may be selected by setting the
  environment variable ``SWIFT_TEST_IN_MEMORY_OBJ`` to a true value.

- encryption may be added to the proxy pipeline by setting the
  environment variable ``SWIFT_TEST_IN_PROCESS_CONF_LOADER`` to
  ``encryption``.

- a 2+1 EC policy may be installed as the default policy by setting the
  environment variable ``SWIFT_TEST_IN_PROCESS_CONF_LOADER`` to
  ``ec``.

- logging to stdout may be enabled by setting ``SWIFT_TEST_DEBUG_LOGS``.

For example, this command would run the in-process mode functional tests with
encryption enabled in the proxy-server::

    SWIFT_TEST_IN_PROCESS=1 SWIFT_TEST_IN_PROCESS_CONF_LOADER=encryption \
        tox -e func

This particular example may also be run using the ``func-encryption``
tox environment::

    tox -e func-encryption

The ``tox.ini`` file also specifies test environments for running other
in-process functional test configurations, e.g.::

  tox -e func-ec

To debug the functional tests, use the 'in-process test' mode and pass the
``--pdb`` flag to ``tox``::

    SWIFT_TEST_IN_PROCESS=1 tox -e func -- --pdb \
        test.functional.tests.TestFile.testCopy

The 'in-process test' mode searches for ``proxy-server.conf`` and
``swift.conf`` config files from which it copies config options and overrides
some options to suit in process testing. The search will first look for config
files in a ``<custom_conf_source_dir>`` that may optionally be specified using
the environment variable::

     SWIFT_TEST_IN_PROCESS_CONF_DIR=<custom_conf_source_dir>

If ``SWIFT_TEST_IN_PROCESS_CONF_DIR`` is not set, or if a config file is not
found in ``<custom_conf_source_dir>``, the search will then look in the
``etc/`` directory in the source tree. If the config file is still not found,
the corresponding sample config file from ``etc/`` is used (e.g.
``proxy-server.conf-sample`` or ``swift.conf-sample``).

When using the 'in-process test' mode ``SWIFT_TEST_POLICY`` may be set to
specify a particular storage policy *name* that will be used for testing as
described above. When set, this policy must exist in the ``swift.conf`` file
and its corresponding ring file must exist in ``<custom_conf_source_dir>`` (if
specified) or ``etc/``. The test setup will set the specified policy to be the
default and use its ring file properties for constructing the test object ring.
This allows in-process testing to be run against various policy types and ring
files.

For example, this command would run the in-process mode functional tests
using config files found in ``$HOME/my_tests`` and policy 'silver'::

 SWIFT_TEST_IN_PROCESS=1 SWIFT_TEST_IN_PROCESS_CONF_DIR=$HOME/my_tests \
    SWIFT_TEST_POLICY=silver tox -e func


S3 API cross-compatibility tests
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The cross-compatibility tests in directory `test/s3api` are intended to verify
that the Swift S3 API behaves in the same way as the AWS S3 API. They should
pass when run against either a Swift endpoint (with S3 API enabled) or an AWS
S3 endpoint.

To run against an AWS S3 endpoint, the `/etc/swift/test.conf` file must be
edited to provide AWS key IDs and secrets. Alternatively, an AWS CLI style
credentials file can be loaded by setting the ``SWIFT_TEST_AWS_CONFIG_FILE``
environment variable, e.g.::

    SWIFT_TEST_AWS_CONFIG_FILE=~/.aws/credentials pytest ./test/s3api

.. note::
  When using ``SWIFT_TEST_AWS_CONFIG_FILE``, the region defaults to
  ``us-east-1`` and only the default credentials are loaded.


------------
Coding Style
------------

Swift uses flake8 with the OpenStack `hacking`_ module to enforce
coding style.

Install flake8 and hacking with pip or by the packages of your
Operating System.

It is advised to integrate flake8+hacking with your editor to get it
automated and not get `caught` by Jenkins.

For example for Vim the `syntastic`_ plugin can do this for you.

.. _`hacking`: https://pypi.org/project/hacking
.. _`syntastic`: https://github.com/scrooloose/syntastic

------------------------
Documentation Guidelines
------------------------

The documentation in docstrings should follow the PEP 257 conventions
(as mentioned in the PEP 8 guidelines).

More specifically:

#.  Triple quotes should be used for all docstrings.
#.  If the docstring is simple and fits on one line, then just use
    one line.
#.  For docstrings that take multiple lines, there should be a newline
    after the opening quotes, and before the closing quotes.
#.  Sphinx is used to build documentation, so use the restructured text
    markup to designate parameters, return values, etc.  Documentation on
    the sphinx specific markup can be found here:
    https://www.sphinx-doc.org/en/master/

To build documentation run::

    pip install -r requirements.txt -r doc/requirements.txt
    sphinx-build -W -b html doc/source doc/build/html

and then browse to doc/build/html/index.html. These docs are auto-generated
after every commit and available online at
https://docs.openstack.org/swift/latest/.

--------
Manpages
--------

For sanity check of your change in manpage, use this command in the root
of your Swift repo::

  ./.manpages

---------------------
License and Copyright
---------------------

You can have the following copyright and license statement at
the top of each source file. Copyright assignment is optional.

New files should contain the current year. Substantial updates can have
another year added, and date ranges are not needed.::

    # Copyright (c) 2013 OpenStack Foundation.
    #
    # Licensed under the Apache License, Version 2.0 (the "License");
    # you may not use this file except in compliance with the License.
    # You may obtain a copy of the License at
    #
    #    http://www.apache.org/licenses/LICENSE-2.0
    #
    # Unless required by applicable law or agreed to in writing, software
    # distributed under the License is distributed on an "AS IS" BASIS,
    # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
    # implied.
    # See the License for the specific language governing permissions and
    # limitations under the License.
