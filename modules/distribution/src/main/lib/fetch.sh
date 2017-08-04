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
  download ch.qos.logback:logback-classic:jar:1.1.3 ./logback
  download ch.qos.logback:logback-core:jar:1.1.3 ./logback
  download com.beust:jcommander:jar:1.32
  download com.google.code.gson:gson:jar:2.2.4
  download com.google.guava:guava:jar:13.0.1
  download com.google.inject:guice:jar:4.0
  download commons-collections:commons-collections:jar:3.2.1
  download commons-configuration:commons-configuration:jar:1.10
  download commons-io:commons-io:jar:2.4
  download io.dropwizard.metrics:metrics-core:jar:3.1.1
  download io.dropwizard.metrics:metrics-graphite:jar:3.1.1
  download javax.inject:javax.inject:jar:1
  download log4j:log4j:jar:1.2.17 ./log4j
  download org.apache.curator:curator-client:jar:2.7.1
  download org.apache.curator:curator-framework:jar:2.7.1
  download org.apache.curator:curator-recipes:jar:2.7.1
  download org.hdrhistogram:HdrHistogram:jar:2.1.8
  download org.mpierce.metrics.reservoir:hdrhistogram-metrics-reservoir:jar:1.1.0
  download org.slf4j:jcl-over-slf4j:jar:1.7.2
  download org.slf4j:log4j-over-slf4j:jar:1.7.12 ./logback
  download org.slf4j:slf4j-api:jar:1.7.12
  download org.slf4j:slf4j-log4j12:jar:1.7.12 ./log4j
  # See https://github.com/apache/incubator/issues/820
  download io.netty:netty:jar:3.9.9.Final

  # Jars for deprecated launching in YARN (in Twill)
  download com.101tec:zkclient:jar:0.3 ./twill
  download com.google.code.findbugs:jsr305:jar:2.0.1 ./twill
  download com.yammer.metrics:metrics-annotation:jar:2.2.0 ./twill
  download com.yammer.metrics:metrics-core:jar:2.2.0 ./twill
  download net.sf.jopt-simple:jopt-simple:jar:3.2 ./twill
  download org.apache.kafka:kafka_2.10:jar:0.8.0 ./twill
  download org.apache.twill:twill-api:jar:0.6.0-incubating ./twill
  download org.apache.twill:twill-common:jar:0.6.0-incubating ./twill
  download org.apache.twill:twill-core:jar:0.6.0-incubating ./twill
  download org.apache.twill:twill-discovery-api:jar:0.6.0-incubating ./twill
  download org.apache.twill:twill-discovery-core:jar:0.6.0-incubating ./twill
  download org.apache.twill:twill-yarn:jar:0.6.0-incubating ./twill
  download org.apache.twill:twill-zookeeper:jar:0.6.0-incubating ./twill
  download org.ow2.asm:asm-all:jar:5.0.2 ./twill
  download org.scala-lang:scala-compiler:jar:2.10.1 ./twill
  download org.scala-lang:scala-library:jar:2.10.1 ./twill
  download org.scala-lang:scala-reflect:jar:2.10.1 ./twill
  download org.xerial.snappy:snappy-java:jar:1.0.5 ./twill

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
  echo "./fetch.sh ahz -Daccumulo.version=1.7.2 -Dhadoop.version=2.7.2 -Dzookeeper.version=3.4.8"
esac

