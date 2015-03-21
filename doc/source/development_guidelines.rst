======================
Development Guidelines
======================

-----------------
Coding Guidelines
-----------------

For the most part we try to follow PEP 8 guidelines which can be viewed
here: http://www.python.org/dev/peps/pep-0008/

There is a useful pep8 command line tool for checking files for pep8
compliance which can be installed with ``easy_install pep8``.

------------------
Testing Guidelines
------------------

Swift has a comprehensive suite of tests that are run on all submitted code,
and it is recommended that developers execute the tests themselves to
catch regressions early.  Developers are also expected to keep the
test suite up-to-date with any submitted code changes.

Swift's suite of unit tests can be executed in an isolated environment
with Tox: http://tox.testrun.org/

To execute the unit tests:

* Install Tox:

  - `pip install tox`

* If you do not have python 2.6 installed (as in 12.04):

  - Add `export TOXENV=py27,pep8` to your `~/.bashrc`

  - `. ~/.bashrc`

* Run Tox from the root of the swift repo:

  - `tox`

  Remarks:
  If you installed using: `cd ~/swift; sudo python setup.py develop`,
  you may need to do: `cd ~/swift; sudo chown -R swift:swift swift.egg-info`
  prior to running tox.
  If you ever encounter DistributionNotFound, try to use `tox --recreate`
  or removing .tox directory to force tox to recreate the dependency list

* Optionally, run only specific tox builds:

  - `tox -e pep8,py26`

The functional tests may be executed against a :doc:`development_saio` or
other running Swift cluster using the command:

- `tox -e func`

The endpoint and authorization credentials to be used by functional tests
should be configured in the ``test.conf`` file as described in the section
:ref:`setup_scripts`.

If the ``test.conf`` file is not found then the functional test framework will
instantiate a set of Swift servers in the same process that executes the
functional tests. This 'in-process test' mode may also be enabled (or disabled)
by setting the environment variable ``SWIFT_TEST_IN_PROCESS`` to a true (or
false) value prior to executing `tox -e func`.

When using the 'in-process test' mode, the optional in-memory
object server may be selected by setting the environment variable
``SWIFT_TEST_IN_MEMORY_OBJ`` to a true value.

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

The environment variable ``SWIFT_TEST_POLICY`` may be set to specify
a particular storage policy *name* that will be used for testing. When set,
this policy must exist in the ``swift.conf`` file and its corresponding ring
file must exist in ``<custom_conf_source_dir>`` (if specified) or ``etc/``. The
test setup will set the specified policy to be the default and use its ring
file properties for constructing the test object ring. This allows in-process
testing to be run against various policy types and ring files.

For example, this command would run the in-process mode functional tests
using config files found in ``$HOME/my_tests`` and policy 'silver'::

 SWIFT_TEST_IN_PROCESS=1 SWIFT_TEST_IN_PROCESS_CONF_DIR=$HOME/my_tests \
    SWIFT_TEST_POLICY=silver tox -e func


------------
Coding Style
------------

Swift use flake8 with the OpenStack `hacking`_ module to enforce
coding style.

Install flake8 and hacking with pip or by the packages of your
Operating System.

It is advised to integrate flake8+hacking with your editor to get it
automated and not get `caught` by Jenkins.

For example for Vim the `syntastic`_ plugin can do this for you.

.. _`hacking`: https://pypi.python.org/pypi/hacking
.. _`syntastic`: https://github.com/scrooloose/syntastic

------------------------
Documentation Guidelines
------------------------

The documentation in docstrings should follow the PEP 257 conventions
(as mentioned in the PEP 8 guidelines).

More specifically:

    1.  Triple quotes should be used for all docstrings.
    2.  If the docstring is simple and fits on one line, then just use
        one line.
    3.  For docstrings that take multiple lines, there should be a newline
        after the opening quotes, and before the closing quotes.
    4.  Sphinx is used to build documentation, so use the restructured text
        markup to designate parameters, return values, etc.  Documentation on
        the sphinx specific markup can be found here:
        http://sphinx.pocoo.org/markup/index.html

Installing Sphinx:
  #. Install sphinx (On Ubuntu: `sudo apt-get install python-sphinx`)
  #. `python setup.py build_sphinx`


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

