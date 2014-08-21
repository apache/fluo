#! /usr/bin/env bash

# Copyright 2014 Fluo authors (see AUTHORS)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
script=$( basename "$SOURCE" )
# Stop: Resolve Script Directory

. "$bin"/config.sh

LOGHOST=$(hostname)
SERVICE="worker"

if [[ -z $HADOOP_PREFIX ]]; then
  echo "HADOOP_PREFIX needs to be set!"
  exit 1
fi

case "$1" in
start-yarn)
  java -cp "$FLUO_HOME/lib/*:$FLUO_HOME/lib/observers/*" io.fluo.cluster.WorkerApp -fluo-home $FLUO_HOME -hadoop-prefix $HADOOP_PREFIX
	;;
start-local)
  java -cp "$FLUO_HOME/lib/*:$FLUO_HOME/lib/observers/*" io.fluo.cluster.WorkerRunnable -config-dir $FLUO_CONF_DIR -log-output $FLUO_LOG_DIR  >${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.err &
	;;
stop-local)
	kill `jps -m | grep WorkerRunnable | cut -f 1 -d ' '`
	;;
*)
	echo $"Usage: $0 start-yarn|start-local|stop-local"
	exit 1
esac

