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

package org.apache.fluo.core.util;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.accumulo.util.ZookeeperUtil;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.impl.Environment;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CuratorUtil {

  private static final Logger log = LoggerFactory.getLogger(CuratorUtil.class);

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  private CuratorUtil() {}

  /**
   * Creates a curator built using Application's zookeeper connection string. Root path will start
   * at Fluo application chroot.
   */
  public static CuratorFramework newAppCurator(FluoConfiguration config) {
    return newCurator(config.getAppZookeepers(), config.getZookeeperTimeout());
  }

  /**
   * Creates a curator built using Fluo's zookeeper connection string. Root path will start at Fluo
   * chroot.
   */
  public static CuratorFramework newFluoCurator(FluoConfiguration config) {
    return newCurator(config.getInstanceZookeepers(), config.getZookeeperTimeout());
  }

  /**
   * Creates a curator built using Fluo's zookeeper connection string. Root path will start at root
   * "/" of Zookeeper.
   */
  public static CuratorFramework newRootFluoCurator(FluoConfiguration config) {
    return newCurator(ZookeeperUtil.parseServers(config.getInstanceZookeepers()),
        config.getZookeeperTimeout());
  }

  /**
   * Creates a curator built using the given zookeeper connection string and timeout
   */
  public static CuratorFramework newCurator(String zookeepers, int timeout) {
    return CuratorFrameworkFactory.newClient(zookeepers, timeout, timeout,
        new ExponentialBackoffRetry(1000, 10));
  }

  public static boolean putData(CuratorFramework curator, String zPath, byte[] data,
      NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    if (policy == null) {
      policy = NodeExistsPolicy.FAIL;
    }

    while (true) {
      try {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
            .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(zPath, data);
        return true;
      } catch (Exception nee) {
        if (nee instanceof KeeperException.NodeExistsException) {
          switch (policy) {
            case SKIP:
              return false;
            case OVERWRITE:
              try {
                curator.setData().withVersion(-1).forPath(zPath, data);
                return true;
              } catch (Exception nne) {

                if (nne instanceof KeeperException.NoNodeException) {
                  // node delete between create call and set data, so try create call again
                  continue;
                } else {
                  throw new RuntimeException(nne);
                }
              }
            default:
              throw (KeeperException.NodeExistsException) nee;
          }
        } else {
          throw new RuntimeException(nee);
        }
      }
    }
  }

  /**
   * Starts the ephemeral node and waits for it to be created
   * 
   * @param node Node to start
   * @param maxWaitSec Maximum time in seconds to wait
   */
  public static void startAndWait(PersistentEphemeralNode node, int maxWaitSec) {
    node.start();
    int waitTime = 0;
    try {
      while (node.waitForInitialCreate(1, TimeUnit.SECONDS) == false) {
        waitTime += 1;
        log.info("Waited " + waitTime + " sec for ephemeral node to be created");
        if (waitTime > maxWaitSec) {
          throw new IllegalStateException("Failed to create ephemeral node");
        }
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(e);
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
