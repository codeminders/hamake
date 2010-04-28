#!/usr/bin/env bash
hadoop fs -rmr /user/$USERNAME/build
hadoop fs -rmr /user/$USERNAME/dist
hadoop fs -rmr /user/$USERNAME/test

hadoop fs -mkdir dist/examples
hadoop fs -mkdir build/lib
hadoop fs -mkdir test/resources/scripts

hadoop fs -put ../*.jar dist/examples
hadoop fs -put /usr/lib/hamake/*.jar build/lib
hadoop fs -put /usr/lib/hamake/*.jar dist
hadoop fs -put ../scripts/*.pig test/resources/scripts

hadoop jar /usr/lib/hamake/hamake-1.0.jar -f file:///usr/local/hamake/hamakefiles/class-size.xml
