/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.accumulo.util;

import java.io.IOException;

import io.fluo.api.config.FluoConfiguration;
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

  // Time period that each client will update ZK with their oldest active timestamp
  // If period is too short, Zookeeper may be overloaded. If too long, garbage collection
  // may keep older versions of table data unnecessarily.
  public static final String ZK_UPDATE_PERIOD_PROP = FluoConfiguration.FLUO_PREFIX
      + ".impl.timestamp.update.period";
  public static long ZK_UPDATE_PERIOD_MS_DEFAULT = 60000;

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
   * Retrieves the oldest active timestamp in Fluo by scanning zookeeper
   * 
   * @param zookeepers Zookeeper connection string
   * @return Oldest active timestamp or oldest possible ts (-1) if not found
   */
  public static long getOldestTimestamp(String zookeepers) {
    long oldestTs = Long.MAX_VALUE;
    boolean nodeFound = false;

    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(zookeepers, 30000, null);

      // Try to find oldest active timestamp of transactors
      String tsRootPath = ZookeeperPath.TRANSACTOR_TIMESTAMPS;
      try {
        if (zk.exists(tsRootPath, false) != null) {
          for (String child : zk.getChildren(tsRootPath, false)) {
            Long ts = LongUtil.fromByteArray(zk.getData(tsRootPath + "/" + child, false, null));
            nodeFound = true;
            if (ts < oldestTs) {
              oldestTs = ts;
            }
          }
        }
      } catch (Exception e) {
        log.error("Failed to get oldest timestamp of transactors from Zookeeper", e);
        return OLDEST_POSSIBLE;
      }

      // If no transactors found, lookup oldest active timestamp set by oracle in zookeeper
      if (nodeFound == false) {
        try {
          byte[] d = zk.getData(ZookeeperPath.ORACLE_CUR_TIMESTAMP, false, null);
          oldestTs = LongUtil.fromByteArray(d);
          nodeFound = true;
        } catch (KeeperException | InterruptedException e) {
          log.error("Failed to get oldest timestamp of Oracle from Zookeeper", e);
          return OLDEST_POSSIBLE;
        }
      }

      // Return oldest possible timestamp if no node found
      if (!nodeFound) {
        return OLDEST_POSSIBLE;
      }
      return oldestTs;
    } catch (IOException e) {
      throw new IllegalStateException(e);
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
