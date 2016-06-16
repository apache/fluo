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

package org.apache.fluo.accumulo.util;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for Zookeeper
 */
public class ZookeeperUtil {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperUtil.class);

  public static final long OLDEST_POSSIBLE = -1;

  private ZookeeperUtil() {}

  /**
   * Parses server section of Zookeeper connection string
   */
  public static String parseServers(String zookeepers) {
    int slashIndex = zookeepers.indexOf("/");
    if (slashIndex != -1) {
      return zookeepers.substring(0, slashIndex);
    }
    return zookeepers;
  }

  /**
   * Parses chroot section of Zookeeper connection string
   *
   * @param zookeepers Zookeeper connection string
   * @return Returns root path or "/" if none found
   */
  public static String parseRoot(String zookeepers) {
    int slashIndex = zookeepers.indexOf("/");
    if (slashIndex != -1) {
      return zookeepers.substring(slashIndex).trim();
    }
    return "/";
  }

  /**
   * Retrieves the GC timestamp, set by the Oracle, from zookeeper
   *
   * @param zookeepers Zookeeper connection string
   * @return Oldest active timestamp or oldest possible ts (-1) if not found
   */
  public static long getGcTimestamp(String zookeepers) {
    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(zookeepers, 30000, null);

      // wait until zookeeper is connected
      long start = System.currentTimeMillis();
      while (!zk.getState().isConnected() && System.currentTimeMillis() - start < 30000) {
        Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
      }

      byte[] d = zk.getData(ZookeeperPath.ORACLE_GC_TIMESTAMP, false, null);
      return LongUtil.fromByteArray(d);
    } catch (KeeperException | InterruptedException | IOException e) {
      log.warn("Failed to get oldest timestamp of Oracle from Zookeeper", e);
      return OLDEST_POSSIBLE;
    } finally {
      if (zk != null) {
        try {
          zk.close();
        } catch (InterruptedException e) {
          log.error("Failed to close zookeeper client", e);
        }
      }
    }
  }
}
