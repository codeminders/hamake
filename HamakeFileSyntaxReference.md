

# Introduction #
Hamake is a lightweight workflow engine for Hadoop. Hamake helps to organize your Hadoop Map Reduce jobs, Pig script and local programs and launch them based on dataflow principles - your tasks will be executed as soon as new data will be availible for them. Contrary to Apache Ant or Make utilities, in hamake-file you declare your data sources and destinations first. Tasks are bound to your data. Task A will be called before task B in case input path of task B intercepts with output path of task A. For a particular task, it will be triggered only if input data has been renewed or validity period of output data has been expired.

# Root Tags Description #

## project ##

This is the root element. All other elements should be whithin this tag. The _project_ has following attributes:
| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| name | the name of the project. | No |
| default | the default dataflow transformation rule to start with when no transformation rules are given as Hamake command-line arguments. | No |

Each project defines one or more dataflow transformation rules. Each transformation rule defines two things:
  1. mapping of your input data onto output data
  1. the way your input data are processed: a) process each input file individually; b) process a set of input items in conjunction.

When running Hamake, you can select which transformation rule(s) you want to start with. When no transformation rules are given, the project's default is used. If there you haven't specified default transformation rule Hamake will start with all root rules.

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [property](#property.md) | 0 or more |
| [fileset](#fileset.md) | 0 or more |
| [file](#file.md) | 0 or more |
| [set](#set.md) | 0 or more |
| [fold](#fold.md) | 0 or more |
| [foreach](#foreach.md) | 0 or more |

## property ##

Sets a property (by name and value), in the project. Properties are named values, which can be used in hamake-file. They work like macros. Once defined, they can be used in data function and task attributes, using the Apache Ant variable syntax ${variable\_name}.

Definition example

```
<property name="dfsroot" value="/dfs" />
<property name="numreducers" value="14" />
```

Usage example:

```
<file path="${dfsroot}/data" />
```

# Dataflow Transformation Rules #

In Hamake you first define a dataflow transformation rules (DTR) along with data that should be processed, after that you define tasks for a concrete dataflow transformation rule. DTR maps your input data onto output data. Currently you have an option between two mapping types:

  1. `<foreach>` - get files one-by-one from input, execute specified task for each input file and save result file to appropriate output location
  1. `<fold>` - get a set of input files as a whole, execute specified task for the set, save resulted file(s) to output location(s)

Understanding DTR:
  * _foreach_ DTR is like one-to-one mapping, from data point of view
  * _foreach_ DTR specify mapping of input files onto each output location
  * _foreach_ DTR allows to combine input files from different locations
  * _fold_ DTR is similar to JOIN operation over input data set
  * _fold_ DTR allows to combine input data sets from different locations
  * _fold_ DTR can produce one or more output items for an input data set

## foreach ##

Dataflow transformation rule which maps a group of files at one location(s) to another location(s). This DTR assumes 1 to 1 file mapping between locations, and can process them incrementally, converting only files which are present at source location(s), but not present or not current at all of destinations. Each input file will produce one output file per output location. Output location file considered to be current if all of the following conditions are satisfied:
  1. Output file is present
  1. Output file time stamp is older than input file
  1. Output file time stamp is older than any of time stamps of files with same name in all dependent directories


| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| name | the name of the DTR. | No |
| disabled | whether this task is disabled. Disabled tasks are ignored. | No |
| delete\_first | boolean, indicating whenever output files should be removed prior to running the job.  Default is True. | No |

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [description](#description.md) | 0 or 1 |
| [dependencies](#dependencies.md) | 0 or 1 |
| [input](#foreach_input.md) | 1 |
| [output](#output.md) | 1 |
| [mapreduce](#mapreduce.md) | one of _mapreduce_, _pig_ or _exec_ is required |
| [pig](#pig.md) | one of _mapreduce_, _pig_ or _exec_ is required |
| [exec](#exec.md) | one of _mapreduce_, _pig_ or _exec_ is required |
| [refused](#refused.md) | 1 |

Example:

```
<foreach name="jar-listings">
   <input>
      <fileset path="${lib}" mask="*.jar"/>
   </input>
   <output>
      <file id="jarListing" path="${output}/jar-listings/${foreach:filename}"/>
   </output>
   <mapreduce jar="${dist}/hamake-examples-1.0.jar" main="com.codeminders.hamake.examples.JarListing">
      <parameter>
         <literal value="${foreach:path}"/>
      </parameter>
      <parameter>
         <reference idref="jarListing"/>
      </parameter>
   </mapreduce>
</foreach>
```

## fold ##

Dataflow transformation rule that defines many-to-many mapping between input data and output data. `<fold>` considers all input file(s) to be a set, and if any of them is newer than any of destination, input dataset will be processed.
In its simplest form, `fold` have one input and one output.

| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| name | the name of the DTR. | No |
| disabled | whether this task is disabled. Disabled tasks are ignored. | No |


Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [description](#description.md) | 0 or 1 |
| [dependencies](#dependencies.md) | 0 or 1 |
| [input](#fold_input.md) | 1 |
| [output](#output.md) | 1 |
| [mapreduce](#mapreduce.md) | one of _mapreduce_, _pig_ or _exec_ is required |
| [pig](#pig.md) | one of _mapreduce_, _pig_ or _exec_ is required |
| [exec](#exec.md) | one of _mapreduce_, _pig_ or _exec_ is required |

Example:
```
<fold name="median">
   <input>
      <file id="medianIn" path="${output}/class-size-histogram"/>
   </input>
   <output>
      <file id="medianOut" path="${output}/class-size-median-bin"/>
   </output>
   <pig script="${scripts}/median.pig">
      <parameter name="infile">
         <reference idref="medianIn"/>
      </parameter>
      <parameter name="outfile">
         <reference idref="medianOut"/>
      </parameter>
   </pig>
</fold>
```

## DTR Data ##

In DTR definition you should specify data you want to be processed. Hamake will launch tasks and will build direct acyclic graph base on these data.

### foreach input ###

This tag defines input set of files for _foreach_ DTR. Foreach will process each file from these set independently.

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [fileset](#fileset.md) | one of _fileset_ or _include_ is required. |
| [include](#include.md) | one of _fileset_ or _include_ is required. |

Attributes:
| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| batch\_size | If the batch\_size is set to zero then there will be at most one task execution, and it will include all input files newer than their corresponding output file(s).  If the batch size is set to one (or omitted), one task will be executed for each input file that is newer than its corresponding output file. If the batch size is set to an integer, that is greater than one, then ceil(N/M) tasks will be executed, where N is the number of input files newer than thier corresponding output file(s), and M is the batch size. | No |

Example:
```
<input>
   <fileset path="${output}/jar-listings"/>
</input>
```

### fold input ###

This tag defines input data set for _fold_ DTR.
_fold_ DTR allows you to specify here one or more of folders, files or filesets.

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [fileset](#fileset.md) | 0 or more |
| [file](#file.md) | 0 or more |
| [set](#set.md) | 0 or more |
| [include](#include.md) | 0 or more |

Example:
```
<input>
   <file id="medianIn" path="${output}/class-size-histogram"/>
</input>
```

### output ###

This tag defines output data for _fold_ and _foreach_ DTR.
In case of\_foreach_Hamake will generate output file for all files from input based on data you specified in output. In case of_fold_Hamake will process your input data as a set in case one element (e.g. file or folder) from output data set isn't fresh.
`<output>` has a single optional attribute -_expiration_. It specifies for now many seconds this output considered valid. In other words, this is maximum allowed time difference between inputs and outputs. The period is a number followed by optional letter. Following letters are understood:
  * s - seconds
  * m - minutes
  * h - hours
  * d - days
  * w - weeks_

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [fileset](#fileset.md) | 0 or more |
| [file](#file.md) | 0 or more |
| [set](#set.md) | 0 or more |
| [include](#include.md) | 0 or more |

Example:
```
<output>
   <file id="medianOut" path="${output}/class-size-median-bin"/>
</output>
```

# The Path To The Data #

To specify file, folder of set in Hamake you should use tags `<file>`, `<fileset>`, `<set>` and `<include>`. The first two functions are used to identify files or directories . `<include>` is used to refer `<file>`, `<fileset>` and `<set>` elements by their id.
With `<set>` tag you can combine other elements in a set. _set_ element guarantees that paths whithin this element will be unique.

## fileset ##

A fileset is a group of files. Files are taken from a specified directory and are matched by mask. If mask is omitted, all the files from specified directory are fit.

| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| id | the ID of the element. | No |
| path | the root directory of this FileSet. | Yes |
| mask |  filename glob mask. | No |
| generation | "generation" of files in fileset. The generation mechanism is described in more depth in Hamake manual. | No |

## file ##

A file or directory on file system. The actual file system depends on the path schema (e.g. `file:///` results in a file or folder on local FS).

| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| id | the ID of the element. | No |
| path | path on file system | Yes |
| generation | "generation" of files in fileset. The generation mechanism is described in more depth in Hamake manual. | No |

## set ##

This element allows you to combine elements `<file>`, `<fileset>`, `<set>` and `<include>`. It has a single attribute _id_.

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [fileset](#fileset.md) | 0 or more |
| [file](#file.md) | 0 or more |
| [set](#set.md) | 0 or more |
| [include](#include.md) | 0 or more |

Example:
```
<set id="set1">
   <fileset path="${output}/jar-listings"/>
   <file path="${output}/class-size-histogram"/>
</set>
```

## include ##

If you have multiple _file_, _set_ or _fileset_ elements that define the same data, you can declare all these elements once and refer them later with _include_ tag. This tag has a single attribure _idref_ where you specify id of an element, that has been declared above.

Example:
```
<fileset id="jarListingIn" path="${lib}" mask="*.jar"/>
...
<include idref="jarListingIn" />
```

# Tasks #

In Hamake there are three kind of tasks available: Hadoop map-reduce job, Pig script and local program (script). Task defines job to be done. Sequence of the task and the number of times it will be called is determined by dataflow transformation rule. Order of arguments that will be passed to Hadoop job, pig script or exec task are determined by order of parameters list in the body of the task.

## mapreduce ##

Launches Hadoop Map Reduce job on the JobTracker that is determined by environment of the running VM (Hamake). This tag has following attributes
| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| jar | path to a jar file that will be run | Yes |
| main | the full name of the main class | Yes |

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [parameter](#parameter.md) | 0 or more |
| [jobconf](#jobconf.md) | 0 or more |

Example:

```
<mapreduce jar="${dist}/hamake-examples-1.0.jar" main="com.codeminders.hamake.examples.JarListingFilter">
   <parameter>
      <literal value="${foreach:path}"/>
   </parameter>
   <parameter>
      <reference idref="filterListing"/>
   </parameter>
</mapreduce>
```

## pig ##

Launches Hadoop Pig script on the JobTracker that is determined by environment of the running VM (Hamake). _pig_ has following attributes
| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| script | path to script file that will be run | Yes |
| jar | path to pig jar file in case you want to use your own build of Pig | No |

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [parameter](#parameter.md) | 0 or more |
| [jobconf](#jobconf.md) | 0 or more |

Example:
```
<pig script="${scripts}/median.pig">
   <parameter name="infile">
      <reference idref="medianIn"/>
   </parameter>
   <parameter name="outfile">
      <reference idref="medianOut"/>
   </parameter>
</pig>
```

## exec ##

Executes script or program locally within the running VM (Hamake). It has following attributes
| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| binary | path to script file or program that will be run | Yes |

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [parameter](#parameter.md) | 0 or more |
| [jobconf](#jobconf.md) | 0 or more |

Example:
```
<exec name="createFile" binary="cp">
   <parameter>
      <literal value="${foreach:path}"/>
   </parameter>
   <parameter>
      <reference idref="${outputFile}"/>
   </parameter>
</exec>
```

## classpath ##

Sometimes your jar files depends on 3rd party libraries. To add those libraries use _classpath_ element. Hamake will use Hadoop feature called [Distributed Cache](http://hadoop.apache.org/common/docs/current/api/org/apache/hadoop/filecache/DistributedCache.html) to add your dependencies to the classpath of your job.

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [fileset](#fileset.md) | 0 or more |
| [file](#file.md) | 0 or more |
| [set](#set.md) | 0 or more |
| [include](#include.md) | 0 or more |

Example:
```
<classpath>
   <fileset path="lib/cluster/mahout-0.3" mask="*.jar" />
</classpath>
```

# Task Parameters #

Arguments are passed to Hadoop Map Reduce job, Pig script or program according to task parameters. Please remember that the order of parameters in task body is important.

## parameter ##

This tag defines one parameter of a task. Whithin this element you should specify one or more `<reference>` or `<literal>` tags. Before launching a task Hamake will process each of nested _reference_ and _literal_ tags with _processing\_function_ and combine output with _concat\_function_. Then it will pass output string as an argument to a task. As a concat function you can specify one of three values: _comma_ - all values will be combined in a comma-separated string; space - separate values by space symbol; append - all values will simply be appended to each other.

For a Pig task you can optionally specify name of parameter with _name_ attribute.

| **Attribute** | **Description** | **Required** |
|:--------------|:----------------|:-------------|
| concat\_function | function that will combine sub-elements or parameter in a string. Valid values are: _space_, _comma_, _append_. Default value is _append_ | No |
| processing\_function | function that will process each sub-element of this parameter. Valid values are: _identity_ (output value as it is), _normalizePath_ (normalizes a path, removing double and single dot path steps). Default value is _identity_ | No |
| name | a name of this parameter that will be passed to Pig | No |

Nested elements:
| **Element name** | **Number of times it can occur** |
|:-----------------|:---------------------------------|
| [reference](#reference.md) | 0 or more |
| [literal](#literal.md) | 0 or more |

Example:
```
<parameter>
   <reference idref="histogramIn"/>
</parameter>
<parameter>
   <reference idref="histogramOut"/>
</parameter>
```

### literal ###

_`<literal>`_ is the sub-element of _parameter_ tag that can contain any string value, an _id_ of some data tag or one of following special variables:

${foreach:path} - in case of foreach, full path to a file that is being currently processed<br>
${foreach:filename} - in case of foreach, name of a file that is being currently processed<br>
${foreach:folder} - in case of foreach, parent folder of a file that is being currently processed<br>
${foreach:basename} - in case of foreach, name of a file without extension that is being currently processed<br>
${foreach:ext} - in case of foreach, extension of a file that is being currently processed<br>
<br>
<table><thead><th> <b>Attribute</b> </th><th> <b>Description</b> </th><th> <b>Required</b> </th></thead><tbody>
<tr><td> value </td><td> value of the parameter </td><td> Yes </td></tr></tbody></table>

Example:<br>
<pre><code>&lt;literal value="${somePath}/${foreach:basename}.${foreach:ext}"/&gt;<br>
</code></pre>

<h3>reference</h3>

<i><code>&lt;reference&gt;</code></i> defines<br>
<br>
This tag is used to refer to a data element.<br>
<br>
<table><thead><th> <b>Attribute</b> </th><th> <b>Description</b> </th><th> <b>Required</b> </th></thead><tbody>
<tr><td> idref </td><td> id of data element (e.g. <code>&lt;file&gt;</code> or <code>&lt;set&gt;</code>) </td><td> Yes </td></tr></tbody></table>

<h2>jobconf</h2>

With this element you can set JobConf parameter of a Hadoop task. This element has following attributes:<br>
<br>
<table><thead><th> <b>Attribute</b> </th><th> <b>Description</b> </th><th> <b>Required</b> </th></thead><tbody>
<tr><td> name </td><td> name of the JobConf parameter </td><td> Yes </td></tr>
<tr><td> value </td><td> value of the JobConf parameter </td><td> Yes </td></tr></tbody></table>

<h1>Task Dependencies</h1>

<h2>dependencies</h2>

In case you want to specify some external data a dataflow transformation rule will depend on, you can use <code>&lt;dependencies&gt;</code> tag whithin <code>&lt;foreach&gt;</code> or <code>&lt;fold&gt;</code> elements.<br>
<br>
Nested elements:<br>
<table><thead><th> <b>Element name</b> </th><th> <b>Number of times it can occur</b> </th></thead><tbody>
<tr><td> <a href='#fileset.md'>fileset</a> </td><td> 0 or more </td></tr>
<tr><td> <a href='#file.md'>file</a> </td><td> 0 or more </td></tr>
<tr><td> <a href='#set.md'>set</a> </td><td> 0 or more </td></tr>
<tr><td> <a href='#include.md'>include</a> </td><td> 0 or more </td></tr></tbody></table>

Example:<br>
<pre><code>&lt;dependencies&gt;<br>
    &lt;file path="${libdir}/somefile.txt" /&gt;<br>
&lt;/dependencies&gt;<br>
</code></pre>

<h1>Exception Handlig</h1>
Sometimes you need to mark incorrect files and do not process them on next run. For this purpose you can use <code>&lt;refused&gt;</code> element<br>
<br>
<h2>refused</h2>

Marks incorrect files for which exception occurred by creating a file with the same name in a trash directory or copying a file to a trash folder. Whether file will be copied or an empty file will be created depends on <i>copy</i> attribute<br>
<br>
Nested elements:<br>
<table><thead><th> <b>Element name</b> </th><th> <b>Number of times it can occur</b> </th></thead><tbody>
<tr><td> <a href='#file.md'>file</a> </td><td> 1 or more </td></tr></tbody></table>

Example:<br>
<pre><code>&lt;refused copy="true"&gt;<br>
     &lt;file path="${data}/refused/FB2MetaExtractorRefused" /&gt;<br>
&lt;/refused&gt;<br>
</code></pre>

<h1>Hamakefile Complete Example</h1>
<pre><code>&lt;?xml version="1.0" encoding="UTF-8"?&gt;<br>
<br>
&lt;project name="test"&gt;<br>
<br>
    &lt;property name="output"  value="build/test"/&gt;<br>
    &lt;property name="lib"     value="build/lib"/&gt;<br>
    &lt;property name="dist"    value="dist/examples"/&gt;<br>
    &lt;property name="scripts" value="test/resources/scripts"/&gt;<br>
    &lt;fileset id="jarListingIn" path="${lib}" mask="*.jar"/&gt;<br>
<br>
    &lt;foreach name="jar-listings"&gt;<br>
        &lt;input&gt;<br>
            &lt;include idref="jarListingIn" /&gt;<br>
        &lt;/input&gt;<br>
        &lt;output&gt;<br>
            &lt;file id="jarListing" path="${output}/jar-listings/${foreach:filename}"/&gt;<br>
        &lt;/output&gt;<br>
        &lt;mapreduce jar="${dist}/hamake-examples-1.0.jar" main="com.codeminders.hamake.examples.JarListing"&gt;<br>
          &lt;parameter&gt;<br>
              &lt;literal value="${foreach:path}"/&gt;<br>
          &lt;/parameter&gt;<br>
          &lt;parameter&gt;<br>
              &lt;reference idref="jarListing"/&gt;<br>
          &lt;/parameter&gt;<br>
        &lt;/mapreduce&gt;<br>
    &lt;/foreach&gt;<br>
<br>
    &lt;foreach name="filter-listing"&gt;<br>
        &lt;input&gt;<br>
            &lt;fileset path="${output}/jar-listings"/&gt;<br>
        &lt;/input&gt;<br>
        &lt;output&gt;<br>
            &lt;file id="filterListing" path="${output}/jar-listings-filtered/${foreach:filename}"/&gt;<br>
        &lt;/output&gt;<br>
        &lt;mapreduce jar="${dist}/hamake-examples-1.0.jar" main="com.codeminders.hamake.examples.JarListingFilter"&gt;<br>
          &lt;parameter&gt;<br>
            &lt;literal value="${foreach:path}"/&gt;<br>
          &lt;/parameter&gt;<br>
          &lt;parameter&gt;<br>
            &lt;reference idref="filterListing"/&gt;<br>
          &lt;/parameter&gt;<br>
        &lt;/mapreduce&gt;<br>
    &lt;/foreach&gt;<br>
<br>
    &lt;fold name="histogram"&gt;<br>
        &lt;input&gt;<br>
            &lt;file id="histogramIn" path="${output}/jar-listings-filtered"/&gt;<br>
        &lt;/input&gt;<br>
        &lt;output&gt;<br>
            &lt;file id="histogramOut" path="${output}/class-size-histogram"/&gt;<br>
        &lt;/output&gt;<br>
        &lt;mapreduce jar="${dist}/hamake-examples-1.0.jar" main="com.codeminders.hamake.examples.ClassSizeHistogram"&gt;<br>
          &lt;parameter&gt;<br>
            &lt;reference idref="histogramIn"/&gt;<br>
          &lt;/parameter&gt;<br>
          &lt;parameter&gt;<br>
            &lt;reference idref="histogramOut"/&gt;<br>
          &lt;/parameter&gt;<br>
        &lt;/mapreduce&gt;<br>
    &lt;/fold&gt;<br>
<br>
    &lt;fold name="median"&gt;<br>
        &lt;input&gt;<br>
            &lt;file id="medianIn" path="${output}/class-size-histogram"/&gt;<br>
        &lt;/input&gt;<br>
        &lt;output&gt;<br>
            &lt;file id="medianOut" path="${output}/class-size-median-bin"/&gt;<br>
        &lt;/output&gt;<br>
        &lt;pig script="${scripts}/median.pig"&gt;<br>
          &lt;parameter name="infile"&gt;<br>
            &lt;reference idref="medianIn"/&gt;<br>
          &lt;/parameter&gt;<br>
          &lt;parameter name="outfile"&gt;<br>
            &lt;reference idref="medianOut"/&gt;<br>
          &lt;/parameter&gt;<br>
        &lt;/pig&gt;<br>
    &lt;/fold&gt;<br>
<br>
&lt;/project&gt;<br>
</code></pre>