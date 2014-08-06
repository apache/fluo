package io.fluo.core.util;

import org.apache.curator.framework.*;
import org.apache.curator.retry.*;

public class CuratorUtil {

  private CuratorUtil() {}

  public static CuratorFramework getCurator(String zookeepers, long timeout) {
    return CuratorFrameworkFactory.newClient(zookeepers, 30000, 30000, new ExponentialBackoffRetry(1000, 10));
  }
}
