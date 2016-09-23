#! /usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.

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

if [ -f "$bin"/../conf/fluo-env.sh ]; then
    . "$bin"/../conf/fluo-env.sh
fi

if [[ "$OSTYPE" == "darwin"* ]]; then
  export MD5=md5
  export SED="sed -i .bak"
else
  export MD5=md5sum
  export SED="sed -i"
fi

. $impl/fluo-version.sh
