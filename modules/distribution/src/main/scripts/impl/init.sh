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

echo "Copying Fluo jars to HDFS at /fluo/lib to be accessible by Accumulo for iterators"
$HADOOP_PREFIX/bin/hdfs dfs -mkdir -p /fluo/lib
echo "Copying `ls $FLUO_HOME/lib/fluo-api-*.jar` to HDFS"
$HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal -f $FLUO_HOME/lib/fluo-api-*.jar /fluo/lib/
echo "Copying `ls $FLUO_HOME/lib/fluo-accumulo-*.jar` to HDFS"
$HADOOP_PREFIX/bin/hdfs dfs -copyFromLocal -f $FLUO_HOME/lib/fluo-accumulo-*.jar /fluo/lib/

java -cp "$FLUO_LIB_DIR/*:$FLUO_LIB_DIR/logback/*:$FLUO_LIB_DIR/observers/*" io.fluo.cluster.init.Init -config-dir $FLUO_CONF_DIR $1
