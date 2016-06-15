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
import java.nio.charset.StandardCharsets;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.util.Halt;

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

  /**
   * Start watching the fluo app uuid. If it changes or goes away then halt the process.
   */
  public static NodeCache startAppIdWatcher(Environment env) {
    try {
      CuratorFramework curator = env.getSharedResources().getCurator();

      byte[] uuidBytes = curator.getData().forPath(ZookeeperPath.CONFIG_FLUO_APPLICATION_ID);
      if (uuidBytes == null) {
        Halt.halt("Fluo Application UUID not found");
        throw new RuntimeException(); // make findbugs happy
      }

      final String uuid = new String(uuidBytes, StandardCharsets.UTF_8);

      final NodeCache nodeCache = new NodeCache(curator, ZookeeperPath.CONFIG_FLUO_APPLICATION_ID);
      nodeCache.getListenable().addListener(new NodeCacheListener() {
        @Override
        public void nodeChanged() throws Exception {
          ChildData node = nodeCache.getCurrentData();
          if (node == null || !uuid.equals(new String(node.getData(), StandardCharsets.UTF_8))) {
            Halt.halt("Fluo Application UUID has changed or disappeared");
          }
        }
      });
      nodeCache.start();
      return nodeCache;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
