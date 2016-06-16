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

package org.apache.fluo.core.worker.finder.hash;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.curator.utils.ZKPaths;
import org.apache.fluo.accumulo.data.MutableBytes;
import org.apache.fluo.accumulo.iterators.NotificationHashFilter;
import org.apache.fluo.accumulo.util.NotificationUtil;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.UtilWaitThread;
import org.apache.fluo.core.worker.NotificationFinder;
import org.apache.fluo.core.worker.NotificationProcessor;
import org.apache.fluo.core.worker.TxResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HashNotificationFinder implements NotificationFinder {

  private NotificationProcessor notificationProcessor;
  private CuratorFramework curator;
  private List<String> finders = Collections.emptyList();
  private int updates = 0;
  private ModulusParams modParams;
  private Environment env;
  private AtomicBoolean stopped = new AtomicBoolean(false);

  private Thread scanThread;
  private PathChildrenCache childrenCache;
  private PersistentEphemeralNode myESNode;

  private static final Logger log = LoggerFactory.getLogger(HashNotificationFinder.class);

  static class ModParamsChangedException extends RuntimeException {
    private static final long serialVersionUID = 1L;
  }

  private class FindersListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
      switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_REMOVED:
          if (!stopped.get()) {
            updateFinders();
          }
          break;
        case CHILD_UPDATED:
          log.warn("unexpected event " + event);
          break;
        default:
          break;

      }
    }

  }

  private synchronized void updateFinders() {

    String me = myESNode.getActualPath();
    while (me == null) {
      UtilWaitThread.sleep(100);
      me = myESNode.getActualPath();
    }
    me = ZKPaths.getNodeFromPath(me);

    List<String> children = new ArrayList<>();
    for (ChildData childData : childrenCache.getCurrentData()) {
      children.add(ZKPaths.getNodeFromPath(childData.getPath()));
    }

    Collections.sort(children);

    if (!finders.equals(children)) {
      int index = children.indexOf(me);
      if (index == -1) {
        this.modParams = null;
        finders = Collections.emptyList();
        log.debug("Did not find self in list of finders " + me);
      } else {
        updates++;
        this.modParams = new ModulusParams(children.indexOf(me), children.size(), updates);
        finders = children;
        log.debug("updated modulus params " + modParams.remainder + " " + modParams.divisor);
      }
    }
  }

  synchronized ModulusParams getModulusParams() {
    return modParams;
  }

  @Override
  public void init(Environment env, NotificationProcessor notificationProcessor) {
    Preconditions.checkState(this.notificationProcessor == null);

    this.notificationProcessor = notificationProcessor;

    this.env = env;
    this.curator = env.getSharedResources().getCurator();

    try {
      myESNode =
          new PersistentEphemeralNode(curator, Mode.EPHEMERAL_SEQUENTIAL, ZookeeperPath.FINDERS
              + "/f-", new byte[0]);
      myESNode.start();
      myESNode.waitForInitialCreate(1, TimeUnit.MINUTES);

      childrenCache =
          new PathChildrenCache(env.getSharedResources().getCurator(), ZookeeperPath.FINDERS, false);
      childrenCache.getListenable().addListener(new FindersListener());
      childrenCache.start(StartMode.BUILD_INITIAL_CACHE);

      updateFinders();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void start() {
    scanThread = new Thread(new ScanTask(this, env, stopped));
    scanThread.setName(getClass().getSimpleName() + " " + ScanTask.class.getSimpleName());
    scanThread.setDaemon(true);
    scanThread.start();
  }

  @Override
  public void stop() {
    stopped.set(true);
    try {
      childrenCache.close();
    } catch (IOException e1) {
      log.warn("Failed to close children cache", e1);
    }

    try {
      myESNode.close();
    } catch (IOException e1) {
      log.warn("Failed to close ephemeral node", e1);
    }

    scanThread.interrupt();
    try {
      scanThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void failedToProcess(Notification notification, TxResult status) {}

  NotificationProcessor getWorkerQueue() {
    return notificationProcessor;
  }

  @VisibleForTesting
  static boolean shouldProcess(Notification notification, int divisor, int remainder) {
    byte[] cfcq = NotificationUtil.encodeCol(notification.getColumn());
    return NotificationHashFilter.accept(
        ByteUtil.toByteSequence((MutableBytes) notification.getRow()), new ArrayByteSequence(cfcq),
        divisor, remainder);
  }

  @Override
  public boolean shouldProcess(Notification notification) {
    ModulusParams mp = getModulusParams();
    return shouldProcess(notification, mp.divisor, mp.remainder);
  }
}
