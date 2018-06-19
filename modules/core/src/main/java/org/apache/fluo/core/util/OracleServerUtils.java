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

import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.curator.shaded.com.google.common.util.concurrent.Uninterruptibles;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.api.config.FluoConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OracleServerUtils {

  private static final Logger log = LoggerFactory.getLogger(OracleServerUtils.class);

  private OracleServerUtils() {}


  /*
   * Gets the Participant from ZooKeeper at the ZookeeperPath.ORACLE_SERVER
   */
  public static Participant getLeadingOracle(FluoConfiguration config) {

    Participant leader = null;

    try (CuratorFramework curator = CuratorUtil.newAppCurator(config)) {
      curator.start();
      try {
        curator.blockUntilConnected();
      } catch (InterruptedException e) {
        log.debug("Interrupted", e);
      }

      LeaderLatch latch = new LeaderLatch(curator, ZookeeperPath.ORACLE_SERVER);

      try {
        latch.start();
        leader = latch.getLeader();
        Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS); // Polling ZK server
        latch.close();
      } catch (Exception e) {
        log.debug("Exception in getOracle", e);
      }

    }
    return leader;
  }

  public static boolean oracleExists(CuratorFramework curator) {
    try {
      return curator.checkExists().forPath(ZookeeperPath.ORACLE_SERVER) != null
          && !curator.getChildren().forPath(ZookeeperPath.ORACLE_SERVER).isEmpty();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
