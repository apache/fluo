/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.accumulo.util;

import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper methods for retrieving oldest active timestamp from Zookeeper
 */
public class ZookeeperUtil {

  private static final Logger log = LoggerFactory.getLogger(ZookeeperUtil.class);

  public static final long OLDEST_POSSIBLE = -1;

  private ZookeeperUtil() {}

  /**
   * Retrieves the oldest active timestamp in Fluo by scanning zookeeper
   * 
   * @param zookeepers Zookeeper connection string
   * @param zkPath Zookeeer root path
   * @return Oldest active timestamp or oldest possible ts (-1) if not found
   */
  public static long getOldestTimestamp(String zookeepers, String zkPath) {
    long oldestTs = Long.MAX_VALUE;
    boolean nodeFound = false;

    ZooKeeper zk = null;
    try {
      zk = new ZooKeeper(zookeepers, 30000, null);

      // Try to find oldest active timestamp of transactors 
      String tsRootPath = ZookeeperConstants.transactorTsRoot(zkPath);
      try {
        if (zk.exists(tsRootPath, false) != null) { 
          for (String child : zk.getChildren(tsRootPath, false)) {
            Long ts = LongUtil.fromByteArray(zk.getData(tsRootPath+"/"+child, false, null));
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
          byte[] d = zk.getData(ZookeeperConstants.oracleCurrentTimestampPath(zkPath), false, null);
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
