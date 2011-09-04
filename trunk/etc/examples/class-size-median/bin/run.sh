#!/bin/bash

PRG="$0"
        
while [ -h "$PRG" ]; do
  ls=`ls -ld "$PRG"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    PRG="$link"
  else
    PRG=`dirname "$PRG"`/"$link"
  fi
done
    
bin=`dirname "$PRG"`
bin=`cd "$bin"; pwd`
BASEDIR="$bin/.."

function print_help {
	echo "usage: $0 <output_folder>" 
}

if [ $# -eq 0 ]; then
	echo "ERROR: please specify path to an output folder"
	print_help
	exit 1
fi 

RUN_FOLDER=$1
shift

if [ "$HADOOP_HOME" == "" ]; then
	echo "ERROR: please set HADOOP_HOME environment variable"
	exit 1
fi

HADOOP_BIN=$HADOOP_HOME/bin/hadoop

if [ ! -x $HADOOP_BIN ]; then
	echo "ERROR: could not find $HADOOP_BIN"
	exit 1
fi

$HADOOP_BIN fs -rmr $RUN_FOLDER

$HADOOP_BIN fs -mkdir $RUN_FOLDER
$HADOOP_BIN fs -mkdir $RUN_FOLDER/result
$HADOOP_BIN fs -mkdir $RUN_FOLDER/data
$HADOOP_BIN fs -mkdir $RUN_FOLDER/scripts

$HADOOP_BIN fs -put $BASEDIR/hamake-examples-${hamake.version}-${release.number}.jar $RUN_FOLDER
$HADOOP_BIN fs -put $BASEDIR/data/*.jar $RUN_FOLDER/data
$HADOOP_BIN fs -put $BASEDIR/scripts/*.pig $RUN_FOLDER/scripts

export RUN_FOLDER
$HADOOP_BIN jar $BASEDIR/hamake-${hamake.version}-${release.number}.jar -f file://$BASEDIR/hamakefiles/class-size.xml
