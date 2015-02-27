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
   impl="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$impl/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
impl="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
script=$( basename "$SOURCE" )
# Stop: Resolve Script Directory

. "$impl"/config.sh

if [[ -z $HADOOP_PREFIX ]]; then
  echo "HADOOP_PREFIX needs to be set!"
  exit 1
fi

if [ ! -d "$HADOOP_PREFIX" ]; then
  echo "HADOOP_PREFIX=$HADOOP_PREFIX is not a valid directory.  Please correct it in your .bashrc or fluo-env.sh"
  exit 1
fi

ADMIN_OPTS="-fluo-conf $FLUO_CONF_DIR -fluo-lib $FLUO_LIB_DIR -hadoop-prefix $HADOOP_PREFIX"
ADMIN_LIB="$FLUO_LIB_DIR/*:$FLUO_LIB_DIR/logback/*"

case "$1" in
start)
  java -cp "$ADMIN_LIB:$FLUO_LIB_DIR/observers/*" io.fluo.cluster.yarn.YarnAdmin $ADMIN_OPTS -command start
	;;
stop)
  java -cp "$ADMIN_LIB" io.fluo.cluster.yarn.YarnAdmin $ADMIN_OPTS -command stop
	;;
kill)
  java -cp "$ADMIN_LIB" io.fluo.cluster.yarn.YarnAdmin $ADMIN_OPTS -command kill
	;;
status)
  java -cp "$ADMIN_LIB" io.fluo.cluster.yarn.YarnAdmin $ADMIN_OPTS -command status
	;;
info)
  java -cp "$ADMIN_LIB" io.fluo.cluster.yarn.YarnAdmin $ADMIN_OPTS -command info
	;;
*)
	echo -e "Usage: fluo yarn <argument>\n"
  echo -e "Possible arguments:\n"
  echo "  start     Start Fluo instance in YARN"
  echo "  stop      Stops Fluo instance in YARN"
  echo "  status    Prints status of Fluo instance in YARN"
  echo "  info      Prints information on Fluo containers"
  echo "  kill      Kills Fluo instance in YARN"
  exit 1
esac
