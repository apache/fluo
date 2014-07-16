#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
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

FLUO_HOME=$( cd -P ${bin}/.. && pwd )
export FLUO_HOME

FLUO_CONF_DIR="${FLUO_CONF_DIR:-$FLUO_HOME/conf}"
export FLUO_CONF_DIR
if [ -z "$FLUO_CONF_DIR" -o ! -d "$FLUO_CONF_DIR" ]
then
  echo "FLUO_CONF_DIR=$FLUO_CONF_DIR is not a valid directory.  Please make sure it exists"
  exit 1
fi

if [ -f $FLUO_CONF_DIR/fluo-env.sh ] ; then
   . $FLUO_CONF_DIR/fluo-env.sh
fi

if [ -z ${FLUO_LOG_DIR} ]; then
   FLUO_LOG_DIR=$FLUO_HOME/logs
fi

mkdir -p $FLUO_LOG_DIR 2>/dev/null

export FLUO_LOG_DIR



