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
package io.fluo.core.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

public class CuratorUtil {

  public enum NodeExistsPolicy {
    SKIP, OVERWRITE, FAIL
  }

  private CuratorUtil() {}

  public static CuratorFramework getCurator(String zookeepers, int timeout) {
    return CuratorFrameworkFactory.newClient(zookeepers, timeout, timeout, new ExponentialBackoffRetry(1000, 10));
  }

  public static boolean putData(CuratorFramework curator, String zPath, byte[] data, NodeExistsPolicy policy) throws KeeperException, InterruptedException {
    if (policy == null)
      policy = NodeExistsPolicy.FAIL;

    while (true) {
      try {
        curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(zPath, data);
        return true;
      } catch (Exception nee) {
        if(nee instanceof KeeperException.NodeExistsException) {
          switch (policy) {
            case SKIP:
              return false;
            case OVERWRITE:
              try {
                curator.setData().withVersion(-1).forPath(zPath, data);
                return true;
              } catch (Exception nne) {

                if(nne instanceof KeeperException.NoNodeException)
                  // node delete between create call and set data, so try create call again
                  continue;
                else
                  throw new RuntimeException(nne);
              }
            default:
              throw (KeeperException.NodeExistsException)nee;
          }
        } else
          throw new RuntimeException(nee);
      }
    }
  }

}
