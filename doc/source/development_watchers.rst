================
Auditor Watchers
================

--------
Overview
--------

The duty of auditors is to guard Swift against corruption in the
storage media. But because auditors crawl all objects, they can be
used to program Swift to operate on every object. It is done through
an API known as "watcher".

Watchers do not have any private view into the cluster.
An operator can write a standalone program that walks the
directories and performs any desired inspection or maintenance.
What watcher brings to the table is a framework to do the same
job easily, under resource restrictions already in place
for the auditor.

Operations performed by watchers are often site-specific, or else
they would be incorporated into Swift already. However, the code in
the tree provides a reference implementation for convenience.
It is located in swift/obj/watchers/dark_data.py and implements
so-called "Dark Data Watcher".

Currently, only object auditor supports the watchers.

-------------
The API class
-------------

The implementation of a watcher is a Python class that may look like this::

  class MyWatcher(object):

    def __init__(self, conf, logger, **kwargs):
        pass

    def start(self, audit_type, **kwargs):
        pass

    def see_object(self, object_metadata, policy_index, partition,
                   data_file_path, **kwargs):
        pass

    def end(self, **kwargs):
        pass

Arguments to watcher methods are passed as keyword arguments,
and methods are expected to consume new, unknown arguments.

The method __init__() is used to save configuration and logger
at the start of the plug-in.

The method start() is invoked when auditor starts a pass.
It usually resets counters. The argument `auditor_type` is string of
`"ALL"` or `"ZBF"`, according to the type of the auditor running
the watcher. Watchers that talk to the network tend to hang off the
ALL-type auditor, the lightweight ones are okay with the ZBF-type.

The method end() is the closing bracket for start(). It is typically
used to log something, or dump some statistics.

The method see_object() is called when auditor completed an audit
of an object. This is where most of the work is done.

The protocol for see_object() allows it to raise a special exception,
QuarantienRequested. Auditor catches it and quarantines the object.
In general, it's okay for watcher methods to throw exceptions, so
an author of a watcher plugin does not have to catch them explicitly
with a try:; they can be just permitted to bubble up naturally.

-------------------
Loading the plugins
-------------------

Swift auditor loads watcher classes from eggs, so it is necessary
to wrap the class and provide it an entry point::

  $ cat /usr/lib/python3.8/site-p*/mywatcher*egg-info/entry_points.txt
  [mywatcher.mysection]
  mywatcherentry = mywatcher:MyWatcher

Operator tells Swift auditor what plugins to load by adding them
to object-server.conf in the section [object-auditor]. It is also
possible to pass parameters, arriving in the argument conf{} of
method start()::

  [object-auditor]
  watchers = mywatcher#mywatcherentry,swift#dark_data

  [object-auditor:watcher:mywatcher#mywatcherentry]
  myparam=testing2020

Do not forget to remove the watcher from auditors when done.
Although the API itself is very lightweight, it is common for watchers
to incur a significant performance penalty: they can talk to networked
services or access additional objects.

-----------------
Dark Data Watcher
-----------------

The watcher API is assumed to be under development. Operators who
need extensions are welcome to report any needs for more arguments
to see_object().

The :ref:`dark_data` watcher has been provided as an example. If an
operator wants to create their own watcher, start by copying
the provided example template ``swift/obj/watchers/dark_data.py`` and see
if it is sufficient.
