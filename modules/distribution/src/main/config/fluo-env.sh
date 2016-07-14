# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements.  See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The ASF licenses this file to you under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the License.  You may
# obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.

# Sets HADOOP_PREFIX if it is not already set.  Please modify the
# export statement to use the correct directory.  Remove the test
# statement to override any previously set environment.

test -z "$HADOOP_PREFIX" && export HADOOP_PREFIX=/path/to/hadoop

# The classpath for Fluo must be defined.  The Fluo tarball does not include
# jars for Accumulo, Zookeeper, or Hadoop.  This example env file offers two
# ways to setup the classpath with these jars.  Go to the end of the file for
# more info.

addToClasspath() 
{
  local dir=$1
  local filterRegex=$2

  if [ ! -d "$dir" ]; then
    echo "ERROR $dir does not exist or not a directory"
    exit 1
  fi

  for jar in $dir/*.jar; do
    if ! [[ $jar =~ $filterRegex ]]; then
       CLASSPATH="$CLASSPATH:$jar"
    fi
  done
}


# This function attemps to obtain Accumulo, Hadoop, and Zookeeper jars from the
# location where those dependencies are installed on the system.
setupClasspathFromSystem()
{
  test -z "$ACCUMULO_HOME" && ACCUMULO_HOME=/path/to/accumulo
  test -z "$ZOOKEEPER_HOME" && ZOOKEEPER_HOME=/path/to/zookeeper

  CLASSPATH="$FLUO_HOME/lib/*:$FLUO_HOME/lib/logback/*"

  #any jars matching this pattern is excluded from classpath
  EXCLUDE_RE="(.*log4j.*)|(.*asm.*)|(.*guava.*)|(.*gson.*)"

  addToClasspath "$ACCUMULO_HOME/lib" $EXCLUDE_RE
  addToClasspath "$ZOOKEEPER_HOME" $EXCLUDE_RE
  addToClasspath "$ZOOKEEPER_HOME/lib" $EXCLUDE_RE
  addToClasspath "$HADOOP_PREFIX/share/hadoop/common" $EXCLUDE_RE;
  addToClasspath "$HADOOP_PREFIX/share/hadoop/common/lib" $EXCLUDE_RE;
  addToClasspath "$HADOOP_PREFIX/share/hadoop/hdfs" $EXCLUDE_RE;
  addToClasspath "$HADOOP_PREFIX/share/hadoop/hdfs/lib" $EXCLUDE_RE;
  addToClasspath "$HADOOP_PREFIX/share/hadoop/yarn" $EXCLUDE_RE;
  addToClasspath "$HADOOP_PREFIX/share/hadoop/yarn/lib" $EXCLUDE_RE;
}


# This function obtains Accumulo, Hadoop, and Zookeeper jars from
# $FLUO_HOME/lib/ahz/. Before using this function, make sure you run
# `./lib/fetch.sh ahz` to download dependencies to this directory.
setupClasspathFromLib(){
  CLASSPATH="$FLUO_HOME/lib/*:$FLUO_HOME/lib/logback/*:$FLUO_HOME/lib/ahz/*"
}

# Call one of the following functions to setup the classpath or write your own
# bash code to setup the classpath for Fluo. You must also run the command
# `./lib/fetch.sh extra` to download extra Fluo dependencies before using Fluo.

setupClasspathFromSystem
#setupClasspathFromLib
