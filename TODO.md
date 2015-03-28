TODO List for hamake project

The main TODO item is to rewrite whole thing in Java, which Hamake process being Hadoop task itself.

Java-related improvements:

  * Start hadoop tasks via Java calls, not spawning processes
  * PIG tasks via Java pig API (embeddable pig)
  * Allow to specify library JARs for Java tasks.
  * hamakefiles could be stored on DFS as well as on local file system
  * Make possible to add new jobs to Amazon EMR instead of creating/executing within one EMR job
  * Implement [event notification](http://www.cascading.org/documentation/features/event-notification.html) (in case of EC2 Amazon SQS can be used)

Misc improvements:
  * Option to stop MAP task if one or certain number file processing failures (now it tries to process all files)
  * Implement file-to-file MAP tasks
  * Task "FS" (operations with/between filesystems: DFS COPY, MOVE etc commands)
  * action on error
  * workflow monitoring
  * Allow to run external any script/command
  * Handle files removal in MAP tasks.
  * Review how REDUCE tasks without inputs of outputs supposed to work
  * in schema specify enums for 'pathparam' 'type' attr.
  * Option to limit number of Hadoop jobs running per file simultaneously for MAP tasks. For example MAP on 10K files now will try to spawn 10K tasks at once. Should be limited have no more than certain number running at the same time. The number could be set via command line or 'config' section
  * Allow to specify -j value via config. (Command line overrides it)
  * integration with Fair Sceduler
  * 'Daemon' mode, where it works as daemon, continuously checking for dependencies and running tasks as necessary. Implies web console for status reporting and control.