/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.cluster.util;

import java.io.File;

@Deprecated
public class ClusterUtil {

  private ClusterUtil() {}

  public static void verifyConfigFilesExist(String configDir, String... fileNames) {
    for (String fn : fileNames) {
      File f = new File(configDir + "/" + fn);
      if (!f.isFile()) {
        System.out.println("ERROR - This command requires the file 'conf/" + fn
            + "' to be present. It can be created by copying its example from 'conf/examples'.");
        System.exit(-1);
      }
    }
  }

  public static void verifyConfigPathsExist(String... paths) {
    for (String path : paths) {
      File f = new File(path);
      if (!f.isFile()) {
        System.out.println("ERROR - This command requires the file '" + path
            + "' to be present. It can be created by copying its example from 'conf/examples'.");
        System.exit(-1);
      }
    }
  }

}
