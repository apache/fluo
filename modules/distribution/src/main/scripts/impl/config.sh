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
bin="$( cd -P "$( dirname "$impl" )" && pwd )"
script=$( basename "$SOURCE" )
# Stop: Resolve Script Directory

. "$bin"/../conf/fluo-env.sh

# Determine FLUO_HOME - Use env variable set by user.  If none set, calculate using bin dir
FLUO_HOME="${FLUO_HOME:-$( cd -P ${bin}/.. && pwd )}"
export FLUO_HOME
if [ -z "$FLUO_HOME" -o ! -d "$FLUO_HOME" ]
then
  echo "FLUO_HOME=$FLUO_HOME is not a valid directory.  Please make sure it exists"
  exit 1
fi

# Determine FLUO_CONF_DIR - Use env variable set by user.  If none set, calculate using FLUO_HOME
FLUO_CONF_DIR="${FLUO_CONF_DIR:-$FLUO_HOME/conf}"
export FLUO_CONF_DIR
if [ -z "$FLUO_CONF_DIR" -o ! -d "$FLUO_CONF_DIR" ]
then
  echo "FLUO_CONF_DIR=$FLUO_CONF_DIR is not a valid directory.  Please make sure it exists"
  exit 1
fi

# Determine FLUO_LIB_DIR - Use env variable set by user.  If none set, calculate using FLUO_HOME
FLUO_LIB_DIR="${FLUO_LIB_DIR:-$FLUO_HOME/lib}"
export FLUO_LIB_DIR
if [ -z "$FLUO_CONF_DIR" -o ! -d "$FLUO_LIB_DIR" ]
then
  echo "FLUO_LIB_DIR=$FLUO_LIB_DIR is not a valid directory.  Please make sure it exists"
  exit 1
fi

# Set up FLUO_LOG_DIR and make sure it exists
if [ -z ${FLUO_LOG_DIR} ]; then
   FLUO_LOG_DIR=$FLUO_HOME/logs
fi
mkdir -p $FLUO_LOG_DIR 2>/dev/null
export FLUO_LOG_DIR
