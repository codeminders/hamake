# HAMAKE #

## Introduction ##

Most non-trivial data processing scenarios with Hadoop typically require more than one MapReduce job. Usually such processing is data-driven, with the data funneled through a sequence of jobs. The processing model could be presented in terms of _dataflow programming_. It could be expressed as a directed graph, with datasets as nodes. Each edge indicates a dependency between two or more datasets and is associated with a processing instruction (Hadoop MapReduce job, PIG Latin script or an external command), which produces one dataset from the others. Using _fuzzy timestamps_ as a way to detect when a dataset needs to be updated, we can calculate a sequence in which the tasks need to be executed to bring all datasets up to date. Jobs for updating independent datasets could be executed concurrently, taking advantage of your Hadoop cluster's full capacity. The dependency graph may even contain cycles, leading to dependency loops which could be resolved using dataset versioning.

These ideas inspired the creation of **HAMAKE** utility. We tried emphasizing data and allowing the developer to express one's goals in terms of _dataflow_ (versus _workflow_). Data dependency graph is expressed using just two data flow instructions: **fold** and **foreach** providing a clear processing model, similar to MapReduce, but on a dataset level. Another design goal was to create a simple to use utility that developers can start using right away without complex installation or extensive learning.

## Key Features ##

  1. Lightweight utility - no need for complex installation
  1. Based on dataflow programming model
  1. Easy learning curve.
  1. Supports Amazon Elastic MapReduce
  1. Allows to run MapReduce jobs as well as PIG Latin scripts

## Installation ##

To install Hamake simply copy hamake-x.x.jar to the directory of your choice and make sure that hadoop command is in your $PATH variable. We also provide RPM and DEB packages.

# Documentation #
  1. QuickStart
  1. HamakeFileSyntaxReference
  1. [FAQ](FAQ.md)
  1. [Hamake Comparison With Other Hadoop Workflow Engines](HamakeComparisonWithOtherWorkflowEngines.md)
  1. HamakeManual
  1. [Whitepaper](http://hamake.googlecode.com/files/hamake_whitepaper.pdf)