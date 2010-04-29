Introduction

Hamake is a lightweight utility for running Hadoop Map Reduce jobs and Pig scrips on a dataset stored on HDFS.
Dataset could be either individual files or directories containing groups of files. Hamake helps to organize
your Hadoop Map Reduce jobs and Pig script and launch them based on dataflow programming model - that means that
your tasks will be executed as soon as new data will be availible for them. To start working with Hamake all you
need is to describe your tasks along with their inputs and outputs locations in hamakefile using simple XML syntax.

Features

   1. lighweight utility - no need of complex installation
   2. based on dataflow programming model
   3. syntax and behavour is similar to Apache Ant 

Installation

To install Hamake simply copy hamake-j-1.0.jar to the directory of your choise and make sure that hadoop command is
in your $PATH variable.

Quick Start

First you should create your hamakefile that desribes tasks, tasks input and output data. Syntax of hamakefile is
described here: http://code.google.com/p/hamake/wiki/SyntaxReference. 

As an example you can take class-size.xml file from example folder of the Hamake distribution. 
To generate data for class-size.xml example, launch:

    examples/bin/start-class-size-example.sh

that script puts content of the 'lib' folder, hamake-examples-1.0.jar and Pig script on HDFS and launches hamake.

In case of successfull execution, you should see /user/$USERNAME/build/test/class-size-histogram/part-00000 and
/user/$USERNAME/build/test/class-size-median-bin/part-00000 files on HDFS

