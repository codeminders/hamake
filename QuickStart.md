version 2.0b-3


# Quick Start #

## Running Hamake Example ##
Hamake runs Hadoop MR jobs, Pig scripts and local scripts based on hamake-file.
As an example you can take class-size.xml hamake file from hamake-examples-2.0b-3.tar.gz archive. To be able to launch examples you should first put Pig jar to the lib folder of Hadoop. To generate data for class-size.xml example, launch following script:
```
bin/start-class-size-example.sh
```
that script puts content of the _hamake-examples-home/data_ folder, hamake-examples-2.0b-3.jar and Pig script on HDFS and launche hamake.
In case of successful execution, you should see /user/$USERNAME/build/test/class-size-histogram/part-00000 and /user/$USERNAME/build/test/class-size-median-bin/part-00000 files on HDFS

## Running Hamake Example on Amazon Elastic MR ##

To run Hamake examples on [Amazon EMR](http://aws.amazon.com/elasticmapreduce/) create  a bucket on s3 (e.g. hamake-examples) and set property _bucket-name_ in the file _hamakefiles/class-size-s3.xml_ to the name of your bucket. Next you should put all resourses needed for running examples on S3:
  1. Copy `<hamake_examples_home>/data/*.jar to s3n://<your_bucket_name>/lib`
  1. Copy `<hamake_home>/hamake-2.0b-3.jar to s3n://<your_bucket_name>/`
  1. Copy `<hamake_examples_home>/examples/hamake-examples-2.0b-3.jar to s3n://<your_bucket_name>/`
  1. Copy `<hamake_examples_home>/hamakefiles/class-size-s3.xml to s3n://<your_bucket_name>/`
  1. Copy `<hamake_examples_home>/scripts/median.pig to s3n://<your_bucket_name>/`
  1. [Download](http://developer.amazonwebservices.com/connect/entry.jspa?externalID=2264&categoryID=266) and [install](http://docs.amazonwebservices.com/ElasticMapReduce/latest/DeveloperGuide/index.html?introduction.html) Amazon EMR command-line tool
  1. Start the Hamake on Amazon EMR:
```
elastic-mapreduce --create --jar s3://<your_bucket_name>/hamake-2.0b-3.jar --main-class com.codeminders.hamake.Main --args -f,s3://<your_bucket_name>/class-size-s3.xml
```

Hamake will start as a single job within Amazon EMR job flow, it will use Amazon's Hadoop configuration and will submit your MR jobs directly to EMR JobTracker.

## Creating Your First Hamake-File ##
This example will give you a short introduction to hamake-file syntax. To get more insight into it, please visit [Hamake-file syntax reference](HamakeFileSyntaxReference.md).

If you haven't done it already, please install hamake. Also please ensure that Hadoop is in your classpath, and that environment variable _HADOOP\_HOME_ points to the root folder of your Hadoop installation. To ensure please run:
```
hadoop
```
If output of the above command is not like
```
Usage: hadoop [--config confdir] COMMAND
where COMMAND is one of:
  namenode -format     format the DFS filesystem
  secondarynamenode    run the DFS secondary namenode
  namenode             run the DFS namenode
  datanode             run a DFS datanode
  dfsadmin             run a DFS admin client
  mradmin              run a Map-Reduce admin client
  fsck                 run a DFS filesystem checking utility
  fs                   run a generic filesystem user client
  balancer             run a cluster balancing utility
  jobtracker           run the MapReduce job Tracker node
  pipes                run a Pipes job
  tasktracker          run a MapReduce task Tracker node
  job                  manipulate MapReduce jobs
  queue                get information regarding JobQueues
  version              print the version
  jar <jar>            run a jar file
  distcp <srcurl> <desturl> copy file or directories recursively
  archive -archiveName NAME <src>* <dest> create a hadoop archive
  daemonlog            get/set the log level for each daemon
 or
  CLASSNAME            run the class named CLASSNAME

```
please run:
```
export HADOOP_HOME=path_to_Hadoop
export PATH=$PATH:$HADOOP_HOME
```
for Linux users or
```
set HADOOP_HOME=path_to_Hadoop
set PATH=%PATH%;%HADOOP_HOME%
```
for Windows users

Your first hamake-file will contain only two (TODO: add link to DTR description) data transformation rules:
  1. DTR that will [generate a text file with random words](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/examples/RandomTextWriter.html).
  1. DTR that will [reads text file and count how often words occur](http://wiki.apache.org/hadoop/WordCount).
These two DTRs will use standart Hadoop classes from org.apache.hadoop.examples package that is located in the hadoop-0.xx.x-examples.jar archive in the root of Hadoop distribution.

Now create file with name _my-first-hamakefile.xml_ and write following two lines to the file:
```
<project name="my-first-project">
</project>
```
With these two lines you start your first project that has a name _my-first-project_.
Every Hamake-file should have at least one data transformation rule. Your first DTR should    generate text file with random context. Add following lines whithin `<project>` tag:
```
<fold name="randomtextwriter">
   <input>      
   </input>
   <output>
      <file id="randomTextWriterOut" path="${hadoop:hadoop.tmp.dir}/randomTextWriter" />
   </output>
   <mapreduce jar="${env:HADOOP_HOME}/hadoop-0.20.2-examples.jar" main="org.apache.hadoop.examples.RandomTextWriter">
      <parameter>
         <reference idref="randomTextWriterOut"/>
      </parameter>
   </mapreduce>
</fold>
```
Here you tell Hamake that it should take zero files as an input and generate file with name randomTextWriter in the temporary folder (the same as Hadoop hadoop.tmp.dir JobConf variable) by running org.apache.hadoop.examples.RandomTextWriter from hadoop-0.xx.x-examples.jar archive. Because this DTR has zero input files it does not depend on other DTRs and will be launched first.

Now you can write a second DTR that will count how often words occur in the random file generated by _randomtextwriter_ DTR. Add following lines above or below _randomtextwriter_ data transformation rule:
```
<fold name="wordcount">
   <input>
      <file id="wordCountIn" path="${hadoop:hadoop.tmp.dir}/randomTextWriter" />
   </input>
   <output>
      <file id="wordCountOut" path="${hadoop:hadoop.tmp.dir}/wordCount" />
   </output>
   <mapreduce jar="${env:HADOOP_HOME}/hadoop-0.20.2-examples.jar" main="org.apache.hadoop.examples.WordCount">
      <parameter>
         <reference idref="wordCountIn"/>
      </parameter>
      <parameter>
         <reference idref="wordCountOut"/>
      </parameter>
   </mapreduce>
</fold>
```

This data transformation rule will launch org.apache.hadoop.examples.WordCount task as soon as org.apache.hadoop.examples.RandomTextWriter task will finish because its input intersects with output of _randomtextwriter_. It will generate one output file in the _wordCount_ folder that will be located in the temporary folder of Hadoop.

To run the example execute command:
```
hadoop jar hamake-2.0b-3.jar -f my-first-hamakefile.xml
```

As an output you should see something like this:
```
10/05/05 17:28:55 INFO hamake.Main: Using Hadoop 0.xx.x
10/05/05 17:28:55 INFO hamake.Main: Working dir:  file:/home/project/HaMake/src/hamake
10/05/05 17:28:55 INFO hamake.Main: Reading hamake-file my-first-hamakefile.xml
10/05/05 17:28:59 INFO hamake.TaskRunner: Starting randomtextwriter
...
10/05/05 17:29:05 WARN hamake.Hamake: Some of your tasks have called System.exit() method. This is not recommended behaviour because it will prevent Hamake from launching other tasks.
10/05/05 17:29:05 INFO hamake.TaskRunner: Execution of randomtextwriter is completed
10/05/05 17:29:05 INFO hamake.TaskRunner: Starting wordcount
...
10/05/05 17:29:10 WARN hamake.Hamake: Some of your tasks have called System.exit() method. This is not recommended behaviour because it will prevent Hamake from launching other tasks.
10/05/05 17:29:10 INFO hamake.TaskRunner: Execution of wordcount is completed
```

# Further Reading #
  1. HamakeFileSyntaxReference
  1. [FAQ](FAQ.md)
  1. HamakeManual