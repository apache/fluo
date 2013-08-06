
test -z "$ACCUMULO_HOME"              && export ACCUMULO_HOME=/path/to/java
test -z "$ACCISMUS_LOG_DIR"           && export ACCISMUS_LOG_DIR=$ACCISMUS_HOME/logs
test -z "$ACCISMUS_WORKER_CLASSPATH"  && export ACCISMUS_WORKER_CLASSPATH="/path/observer1.jar,/path/observer2.jar"

