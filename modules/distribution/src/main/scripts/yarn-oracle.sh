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

if [[ -z $HADOOP_PREFIX ]]; then
  echo "HADOOP_PREFIX needs to be set!"
  exit 1
fi

ADMIN_OPTS="-fluo-home $FLUO_HOME -hadoop-prefix $HADOOP_PREFIX -application FluoOracle"

case "$1" in
start)
  java -cp "$FLUO_HOME/lib/*" io.fluo.cluster.ClusterAdmin $ADMIN_OPTS -command start
	;;
stop)
  java -cp "$FLUO_HOME/lib/*" io.fluo.cluster.ClusterAdmin $ADMIN_OPTS -command stop
	;;
kill)
  java -cp "$FLUO_HOME/lib/*" io.fluo.cluster.ClusterAdmin $ADMIN_OPTS -command kill
	;;
status)
  java -cp "$FLUO_HOME/lib/*" io.fluo.cluster.ClusterAdmin $ADMIN_OPTS -command status
	;;
*)
	echo $"Usage: $0 start|stop|kill|status"
	exit 1
esac
