package io.fluo.core.util;

import org.apache.curator.framework.*;
import org.apache.curator.retry.*;

public class CuratorUtil {

  private CuratorUtil() {}

  public static CuratorFramework getCurator(String zookeepers, int timeout) {
    return CuratorFrameworkFactory.newClient(zookeepers, timeout, timeout, new ExponentialBackoffRetry(1000, 10));
  }
}
