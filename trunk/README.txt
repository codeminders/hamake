Hadoop "Make" utility
=====================

[This is a brief description. Visit project page where eventually more
detailed documentation will be published]

'hamake' utility allows you to automate incremental processing of
datasets stored on HDFS using Hadoop tasks written in Java or using
PigLatin scripts. Datasets could be either individual files or
directories containing groups of files. New files may be added (or
removed) at arbitrary location which may trigger recalculation of data
depending on them.

First, you formulate you processing model in terms of data locations
(which could be used either as inputs or outputs) and tasks.

Currently two types of tasks supported (although they are called "map"
and "reduce" but they should not be confused with Hadoop "map" and
"reduce"):

MAP - this a type of task which maps a group of files at one location
to another location(s). This task assumes 1 to 1 file mapping between
locations, and can process them incrementally, converting only files
which are present at source location, but not at all of destinations.

If we view MAP as a function, we can define it using Haskell language
syntax as:

map:: Path -> [Path] -> [Path]
map:: source, dependencies, targets = ...


REDUCE - this a type of task which takes a group of files as an input
and produce one or more outputs. All input files are considered to be
a dataset, and if any of them is newer than destination, the
re-calculation will be triggered.

If we view REDUCE as a function, we can define it using Haskell
language syntax as:

reduce:: [Path] -> [Path]
reduce:: source, targets = ...

You describe your tasks along with their inputs and outputs locations
in 'hamakefile' using simple XML syntax. See 'sample_hamakefile.xml'
for an example. 'hamake' reads this file, builds dependency graph and
attempts to execute tasks in order which allows to resolve all
dependencies. (in the situation where you have a circular dependency,
you can specify a "generation" attribute on an input or output).
hamakes takes care of figuring out what tasks have to be executed and
in what order. It could execute several tasks in parallel if they do
not depend on each other. It takes care cleaning up results of partial
execution, in case of error.

Requirements:
-------------

* Hadoop-0.18.3
* Python 2.5
* Pig-0.2.0 (optional)
* Hadoop Thrift Server (from Hadoop-0.19 contrib, back-ported to 0.18.3)

Installation:
-------------

python setup.py install

Run:
----

You need to have Hadoop Thrift Server running and host and port of it
should be specified in hamakefile in <thriftAPI> element.

hamake [--dry-run] [-j N] [--verbose] [--test] [-f hamakefile.xml] [<target> ...]

--dry-run : Perform execution simulation without actually executing any tasks or modifying any files.
-j N: execute up to N tasks simultaneously. Applies only to independent tasks. By default N is unlimited. -j 1 will make it execute tasks one by one.
--verbose: be more verbose - report more information on what is being done.
--test: mostly for developers - make it print detailed stack traces in case of errors
-f hamakefile.xml: specifies makefile name. If this option is omitted 'hamakefile.xml' is assumed
target: list of targets (task names) to be executed. Only these targets and (ones they depend on) will be executed.


Author:

Vadim Zaliva lord@crocodile.org
Project URL: http://code.google.com/p/hamake/
Discussion Group: http://groups.google.com/group/hamake-users



