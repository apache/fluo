
test -z "$ACCUMULO_HOME"              && export ACCUMULO_HOME=/path/to/java
test -z "$ACCISMUS_LOG_DIR"           && export ACCISMUS_LOG_DIR=$ACCISMUS_HOME/logs
#set ACCISMUS_WORKER_CLASSPATH to a comma separated list of jars, defaults to jars in $ACCISMUS_HOME/observers
test -z "$ACCISMUS_WORKER_CLASSPATH"  && export ACCISMUS_WORKER_CLASSPATH=$(ls --format=commas $ACCISMUS_HOME/lib/observers/*.jar | tr -d "\n")

