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

import org.apache.curator.framework.CuratorFramework;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.zookeeper.KeeperException;

public class OracleServerUtils {

  private OracleServerUtils() {}

  /**
   * Checks to see if an Oracle Server exists.
   * 
   * @param curator It is the responsibility of the caller to ensure the curator is started
   * @return boolean if the server exists in zookeeper
   */
  public static boolean oracleExists(CuratorFramework curator) {
    boolean exists = false;
    try {
      exists = curator.checkExists().forPath(ZookeeperPath.ORACLE_SERVER) != null
          && !curator.getChildren().forPath(ZookeeperPath.ORACLE_SERVER).isEmpty();
    } catch (Exception nne) {
      if (nne instanceof KeeperException.NoNodeException) {
        // you'll do nothing
      } else {
        throw new RuntimeException(nne);
      }
    }
    return exists;
  }
}
