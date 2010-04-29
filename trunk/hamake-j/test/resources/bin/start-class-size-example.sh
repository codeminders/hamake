#!/usr/bin/env bash

#export HAMAKE_HOME=

hadoop fs -rmr /user/$USERNAME/build
hadoop fs -rmr /user/$USERNAME/dist
hadoop fs -rmr /user/$USERNAME/test

hadoop fs -mkdir dist/examples
hadoop fs -mkdir build/lib
hadoop fs -mkdir test/resources/scripts

hadoop fs -put $HAMAKE_HOME/examples/*.jar dist/examples
hadoop fs -put $HAMAKE_HOME/*.jar build/lib
hadoop fs -put $HAMAKE_HOME/*.jar dist
hadoop fs -put $HAMAKE_HOME/examples/scripts/*.pig test/resources/scripts

hadoop jar $HAMAKE_HOME/hamake-1.0.jar -f file://$HAMAKE_HOME/examples/hamakefiles/class-size.xml
