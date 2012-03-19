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

* Run Tox from the root of the swift repo:

  - `tox`

* Optionally, run only specific tox builds:

  - `tox -e pep8,py26`

------------------------
Documentation Guidelines
------------------------

The documentation in docstrings should follow the PEP 257 conventions 
(as mentioned in the PEP 8 guidelines).

More specifically:

    1.  Triple qutes should be used for all docstrings.
    2.  If the docstring is simple and fits on one line, then just use
        one line.
    3.  For docstrings that take multiple lines, there should be a newline
        after the opening quotes, and before the closing quotes.
    4.  Sphinx is used to build documentation, so use the restructured text
        markup to designate parameters, return values, etc.  Documentation on
        the sphinx specific markup can be found here:
        http://sphinx.pocoo.org/markup/index.html

---------------------
License and Copyright
---------------------

Every source file should have the following copyright and license statement at
the top::

    # Copyright (c) 2010-2012 OpenStack, LLC.
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
