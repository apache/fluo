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

LOGHOST=$(hostname)

LOCAL_OPTS="-config-dir $FLUO_CONF_DIR -log-output $FLUO_LOG_DIR"
LOCAL_LIB="$FLUO_LIB_DIR/*:$FLUO_LIB_DIR/logback/*"

case "$1" in
start-oracle)
  SERVICE="oracle"
  java -cp "$LOCAL_LIB" io.fluo.cluster.FluoOracleMain $LOCAL_OPTS >${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.err &
	;;
stop-oracle)
	kill `jps -m | grep FluoOracleMain | cut -f 1 -d ' '`
	;;
start-worker)
  SERVICE="worker"
  java -cp "$LOCAL_LIB:$FLUO_LIB_DIR/observers/*" io.fluo.cluster.FluoWorkerMain $LOCAL_OPTS >${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.out 2>${FLUO_LOG_DIR}/${SERVICE}_${LOGHOST}.err &
	;;
stop-worker)
	kill `jps -m | grep FluoWorkerMain | cut -f 1 -d ' '`
	;;
*)
	echo -e "Usage: fluo local <argument>\n"
  echo -e "Possible arguments:\n"
  echo "  start-oracle     Start Fluo oracle on local machine"
  echo "  start-worker     Start Fluo worker on local machine"
  echo "  stop-oracle      Stops Fluo oracle on local machine"
  echo "  stop-worker      Stops Fluo worker on local machine"
  echo " " 
  exit 1
esac
