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

# This script will regenerate the thrift code for Fluo's RPC mechanisms.

fail() {
  echo "$@"
  exit 1
}

attachLicense() {
  # concatenate the below license with the input file, $1, then overwrite it
  cat - "$1" > "$1-tmp" <<EOF
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
EOF
  [[ $? -eq 0 ]] || fail unable to attach license
  mv -f "$1-tmp" "$1" || fail unable to replace file after attaching license
}

getThriftVersionFromPom() {
  local returnCodes
  xmllint --shell "../../pom.xml" \
    <<<'xpath /*[local-name()="project"]/*[local-name()="properties"]/*[local-name()="thrift.version"]/text()' \
    | grep content= \
    | cut -f2 -d=
  returnCodes=( "${PIPESTATUS[@]}" )
  for x in "${returnCodes[@]}"; do
    [[ $x -eq 0 ]] || return $x
  done
}

requireThriftVersion() {
  # test to see if thrift is installed; $1 is the expected version
  local ver
  ver=$(thrift -version 2>/dev/null | grep -cF "$1")
  if [[ $ver != '1' ]] ; then
    echo "****************************************************"
    echo "*** thrift is not available"
    echo "***   expecting 'thrift -version' to return $1"
    echo "*** generated code will not be updated"
    fail "****************************************************"
  fi
}

checkThriftVersion() {
  local v; v=$(getThriftVersionFromPom 2>/dev/null) \
    || fail could not determine expected version of thrift from pom.xml file
  requireThriftVersion "$v"
}

generateJava() {
  # output directory is $1/gen-java; the rest of the args are *.thrift files
  local t; t=$1; shift
  mkdir -p "$t"
  rm -rf "$t"/gen-java
  for f in "$@"; do
    thrift -o "$t" --gen java:generated_annotations=suppress "$f" \
      || fail unable to generate java thrift classes
  done
}

suppressWarnings() {
  # function skipped, unless needed
  return
  # shellcheck disable=SC1004 # https://www.shellcheck.net/wiki/SC1004
  # add dummy method to suppress "unnecessary suppress warnings" for classes
  # which don't have any unused variables; this only affects classes, enums
  # aren't affected
  find "$1" -type f -name '*.java' -exec grep -Zl '^public class ' {} + \
    | xargs -0 sed -i -e 's/^[}]$/  private static void unusedMethod() {}\
}/'
}

addLicenseHeaders() {
  find "$1" -type f -name '*.java' -exec "$0" 'attachLicense' '{}' ';'
}

moveSingleFile() {
  # compare files with the destination, and only copy those that have changed
  local src; src=$3
  local dst; dst=$2/${src#$1/}
  mkdir -p "$(dirname "$dst")"
  if ! cmp -s "$src" "$dst" ; then
    echo cp -f "$src" "$dst"
    cp -f "$src" "$dst" || fail unable to copy files to java workspace
  fi
}

moveGeneratedFiles() {
  # copy generated files found in $1 to $2
  find "$1" -type f -name '*.java' -exec "$0" 'moveSingleFile' "$1" "$2" '{}' ';'
}

if [[ $1 == 'attachLicense' || $1 == 'moveSingleFile' ]]; then
  # this enables the function to be used in a find loop
  "$@"
else
  checkThriftVersion
  generateJava target src/main/thrift/*.thrift
  suppressWarnings target/gen-java
  addLicenseHeaders target/gen-java
  moveGeneratedFiles target/gen-java src/main/java
fi
