

# General #
## What is the difference between Hamake and Cascading? ##

In short: [Cascading](http://www.cascading.org/) is an API, while 'hamake' is an utility. Some differences:
  * hamake does not require any custom programming. It helps to automate running your existing Hadoop tasks and PIG scripts
  * We found hamake especially suitable for incremental processing of datasets
  * You can use 'hamake' to automate tasks written in other languages, for example using _Hadoop streaming_

For more detailed comparison please visit HamakeComparisonWithOtherWorkflowEngines

## How Hamake differs from Oozie and Azkaban? ##

Oozie and Azkaban are server-side systems that have to be installed and run as a service. Hamake is a lightweight client-side utility that does not require installation and has very simple syntax for workflow definition.  Most importantly, Hamake is built based on dataflow programming principles - your Hadoop tasks execution sequence is controlled by the data.

For more detailed comparison please visit HamakeComparisonWithOtherWorkflowEngines

## Why XML and not JSON? ##

XML is broadly used in workflow processes description (e.g. BPEL, XPDL)

## Is there a way to query a workflow's progress to get a percentage complete? ##

Currently no.
You can watch log messages on console or in hamake log file to monitor the progress.

## Why not ANT? ##

[Ant is not a workflow engine](http://wiki.apache.org/ant/AntWeaknessesAndProblems)

## Where executables are executed? ##

_exec_ task launches binaries and shell scripts locally on a machine, where Hamake is running. _mapred_ and _pig_ jobs are sent to JobTracker that you've configured