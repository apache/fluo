#!/bin/sh

if [ -z "$ACCUMULO_HOME" -o ! -d "$ACCUMULO_HOME" ]; then
   echo "ACCUMULO_HOME is not set or is not a directory."
   exit 1
fi

if [ -z "$ACCISMUS_HOME" -o ! -d "$ACCISMUS_HOME" ]; then
   echo "ACCISMUS_HOME is not set or is not a directory."
   exit 1
fi


if [ $# -ne 1 ]
then
  echo "Usage: `basename $0` <observer classpath>"
  exit 1
fi

$ACCUMULO_HOME/bin/tool.sh $ACCISMUS_HOME/lib/accismus-0.0.1-SNAPSHOT.jar org.apache.accumulo.accismus.tools.WorkerTool -libjars "$1,$ACCISMUS_HOME/lib/accismus-0.0.1-SNAPSHOT.jar" $ACCISMUS_HOME/conf/accismus.properties

