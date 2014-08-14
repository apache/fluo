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
