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

#this functions expands globs(filtering non jars) and replaces : with ,
function expand_libjars(){
  local arr=$(echo "$cp" | tr ":" "\n")

  libjars=""

  for x in $arr
  do
    libjars="$libjars$(ls $x | grep '.jar$' | tr '\n' ',')"
  done

  #remove trailing ,
  #libjars=${libjars::-1}
  libjars=$(echo "$libjars" | rev | cut -c 2- | rev)
}

function print_usage() {
  echo "Usage : fluo classpath [-a|--accumulo] [-z|--zookeper] [-l|--libjars] [-h|--help]"
  echo 
  echo " This command prints the classpath needed to execute Fluo clients"
  echo
  echo " Option descriptions :"
  echo "       -a|--accumulo     Additionally prints Accumulo client jars"
  echo "       -z|--zookeeper    Additionally prints Zookeeper client jars"
  echo "       -l|--lib-jars     Prints classpath in format suitable for Hadoop -libjars"
  echo "       -h|--help         Print this help message"
  echo
}

fluo_cp="$FLUO_LIB_DIR/fluo-client/*"
accumulo_cp=""
zookeeper_cp=""
call_el=false

TEMP=`getopt -o azlh --long accumulo,zookeeper,lib-jars,help -n 'fluo classpath' -- "$@"`

if [ $? -ne 0 ]
then
  print_usage
  exit 1
fi

eval set -- "$TEMP"

# extract options and their arguments into variables.
while true ; do
    case "$1" in
        -a|--accumulo)
          accumulo_cp=":$FLUO_LIB_DIR/accumulo/*"
          shift
          ;;
        -z|--zookeeper)
          zookeeper_cp=":$FLUO_LIB_DIR/zookeeper/*"
          shift 
          ;;
        -l|--lib-jars)
          call_el=true
          shift 
          ;;
        -h|--help)
          print_usage
          exit 0
          ;;
        --) 
          shift
          break 
          ;;
        *)
         exit 1
         ;;
    esac
done

cp="$fluo_cp$accumulo_cp$zookeeper_cp"

if [ "$call_el" = true ]
then
  expand_libjars
  echo $libjars  
else
  echo "$cp"
fi

