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

lib_dir=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
maven_prefix=https://repo1.maven.org/maven2

function download {
  IFS=':' read -ra DEP <<< "$1"
  dir=$lib_dir/
  if [ -n "$2" ]; then
    dir=$lib_dir/$2
    if [ ! -d $dir ]; then
      mkdir $dir
    fi
  fi
  group=${DEP[0]}
  artifact=${DEP[1]}
  ftype=${DEP[2]}
  version=${DEP[3]}
  fn=$artifact-$version.$ftype
  path="${group//.//}/$artifact/$version/$fn"
  download_url=$maven_prefix/$path

  if [ -f $dir/$fn ]; then
    echo "SUCCESS: Dependency exists - $dir/$fn"
  else
    wget -q $download_url -P $dir
    if [ $? == 0 ]; then
      echo "SUCCESS: Dependency downloaded from $download_url"
    else
      echo "ERROR: Dependency download failed - $download_url"
    fi
  fi
}

case "$1" in
ahz)
  echo "Fetching Hadoop, Accumulo, & Zookeeper dependencies"
  cd $lib_dir/ahz
  rm -f *.jar
  mvn clean package ${*:2}
  ;;
extra)
  echo "Fetching extra Fluo dependencies"
  download aopalliance:aopalliance:jar:1.0
  download com.beust:jcommander:jar:1.82
  download com.google.code.gson:gson:jar:2.9.1
  download com.google.guava:guava:jar:31.1-jre
  download com.google.inject:guice:jar:4.2.3
  download org.apache.commons:commons-collections4:jar:4.4
  download org.apache.commons:commons-configuration2:jar:2.8.0
  download commons-io:commons-io:jar:2.11.0
  download io.dropwizard.metrics:metrics-core:jar:3.2.6
  download io.dropwizard.metrics:metrics-graphite:jar:3.2.6
  download javax.inject:javax.inject:jar:1
  download org.apache.curator:curator-client:jar:4.3.0
  download org.apache.curator:curator-framework:jar:4.3.0
  download org.apache.curator:curator-recipes:jar:4.3.0
  download org.hdrhistogram:HdrHistogram:jar:2.1.10
  download org.mpierce.metrics.reservoir:hdrhistogram-metrics-reservoir:jar:1.1.0
  download org.slf4j:jcl-over-slf4j:jar:2.0.3
  download org.slf4j:slf4j-api:jar:2.0.3

  download org.apache.logging.log4j:log4j-api:jar:2.19.0 ./log4j
  download org.apache.logging.log4j:log4j-core:jar:2.19.0 ./log4j
  download org.apache.logging.log4j:log4j-slf4j2-impl:jar:2.19.0 ./log4j

  echo -e "Done!\n"
  echo "NOTE - The dependencies downloaded have been tested with some versions of Hadoop, Zookeeper, and Accumulo."
  echo "There is no guarantee they will work with all versions. Fluo chose to defer dependency resolution to as"
  echo "late as possible in order to make it easier to resolve dependency conflicts.  If you run into a dependency"
  echo "conflict in your environment, please consider bringing it up on the Fluo dev list."
  ;;
*)
  echo -e "Usage: fetch.sh <DEPENDENCIES>\n"
  echo -e "where <DEPENDENCIES> can be the following:\n"
  echo "   ahz       Download Accumulo, Hadoop, Zookeeper dependencies"
  echo -e "   extra     Download extra Fluo dependencies\n"
  echo "For 'ahz', the versions of Hadoop, Accumulo, & Zookeeper are specifed in ahz/pom.xml."
  echo -e "However, you can override them using the command below:\n"
  echo "./fetch.sh ahz -Daccumulo.version=2.0.0 -Dhadoop.version=3.1.1 -Dzookeeper.version=3.4.14"
esac
