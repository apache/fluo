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

package org.apache.fluo.core.worker;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import com.codahale.metrics.Gauge;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.observer.Observers;
import org.apache.fluo.core.util.FluoExecutors;
import org.apache.fluo.core.util.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationProcessor implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(NotificationProcessor.class);

  private NotificationTracker tracker;
  private ThreadPoolExecutor executor;
  private Environment env;
  private Observers observers;
  private PriorityBlockingQueue<Runnable> queue;

  public NotificationProcessor(Environment env) {
    int numThreads = env.getConfiguration().getWorkerThreads();
    this.env = env;
    this.queue = new PriorityBlockingQueue<>();
    this.executor = FluoExecutors.newFixedThreadPool(numThreads, queue, "ntfyProc");
    this.tracker = new NotificationTracker();
    this.observers = env.getConfiguredObservers().getObservers(env);
    env.getSharedResources().getMetricRegistry()
        .register(env.getMetricNames().getNotificationQueued(), new Gauge<Integer>() {
          @Override
          public Integer getValue() {
            return queue.size();
          }
        });
  }

  // little utility class that tracks all notifications in queue
  private class NotificationTracker {
    private Map<RowColumn, Future<?>> queuedWork = new HashMap<>();
    private Set<RowColumn> recentlyDeleted = new HashSet<>();
    private long sizeInBytes = 0;
    private Map<Long, Predicate<RowColumn>> memoryPredicates = new HashMap<>();
    private Predicate<RowColumn> memoryPredicate = rc -> false;
    private static final long MAX_SIZE = 1 << 24;
    private long nextSessionId = 0;

    private long size(RowColumn rowCol) {
      Column col = rowCol.getColumn();
      return rowCol.getRow().length() + col.getFamily().length() + col.getQualifier().length()
          + col.getVisibility().length();
    }

    public synchronized boolean add(RowColumn rowCol, Future<?> task) {

      if (queuedWork.containsKey(rowCol) || recentlyDeleted.contains(rowCol)) {
        return false;
      }

      while (sizeInBytes > MAX_SIZE) {
        try {
          wait(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }

      if (queuedWork.containsKey(rowCol) || recentlyDeleted.contains(rowCol)) {
        return false;
      }

      queuedWork.put(rowCol, task);
      sizeInBytes += size(rowCol);
      return true;
    }

    public synchronized void remove(RowColumn rowCol) {
      if (queuedWork.remove(rowCol) != null) {
        if (memoryPredicate.test(rowCol)) {
          recentlyDeleted.add(rowCol);
        }
        sizeInBytes -= size(rowCol);
        notify();
      }
    }

    public synchronized void clear() {
      for (Future<?> task : queuedWork.values()) {
        task.cancel(false);
      }

      queuedWork.clear();
      sizeInBytes = 0;
      notify();
    }

    public boolean requeue(RowColumn rowCol, FutureTask<?> ft) {
      if (!queuedWork.containsKey(rowCol)) {
        return false;
      }

      queuedWork.put(rowCol, ft);

      return true;
    }

    private void resetMemoryPredicate() {
      memoryPredicate = null;
      for (Predicate<RowColumn> p : this.memoryPredicates.values()) {
        if (memoryPredicate == null) {
          memoryPredicate = p;
        } else {
          memoryPredicate = p.or(memoryPredicate);
        }
      }
    }

    public synchronized long beginAddingNotifications(Predicate<RowColumn> memoryPredicate) {
      long sessionId = nextSessionId++;
      this.memoryPredicates.put(sessionId, Objects.requireNonNull(memoryPredicate));
      resetMemoryPredicate();
      return sessionId;
    }

    public synchronized void finishAddingNotifications(long sessionId) {
      this.memoryPredicates.remove(sessionId);
      if (memoryPredicates.size() == 0) {
        recentlyDeleted.clear();
        memoryPredicate = rc -> false;
      } else {
        resetMemoryPredicate();
      }
    }

  }

  private class NotificationProcessingTask implements Runnable {

    Notification notification;
    NotificationFinder notificationFinder;
    WorkTaskAsync workTask;

    NotificationProcessingTask(Notification n, NotificationFinder nf, WorkTaskAsync wt) {
      this.notification = n;
      this.notificationFinder = nf;
      this.workTask = wt;
    }

    @Override
    public void run() {
      try {
        // Its possible that while the notification was in the queue the situation changed and it
        // should no longer be processed by this worker. So ask as late as possible if this
        // notification should be processed.
        if (notificationFinder.shouldProcess(notification)) {
          workTask.run();
        } else {
          notificationProcessed(notification);
        }
      } catch (Exception e) {
        log.error("Failed to process work " + Hex.encNonAscii(notification), e);
      }
    }

  }

  private class FutureNotificationTask extends FutureTask<Void> implements
      Comparable<FutureNotificationTask> {

    private final Notification notification;

    public FutureNotificationTask(Notification n, NotificationFinder nf, WorkTaskAsync wt) {
      super(new NotificationProcessingTask(n, nf, wt), null);
      this.notification = n;
    }

    @Override
    public int compareTo(FutureNotificationTask o) {
      return Long.compare(notification.getTimestamp(), o.notification.getTimestamp());
    }

    @Override
    protected void setException(Throwable t) {
      super.setException(t);
      System.err.println("Uncaught Exception ");
      t.printStackTrace();
    }
  }

  public class Session implements AutoCloseable {
    private long id;

    public Session(Predicate<RowColumn> memoryPredicate) {
      this.id = tracker.beginAddingNotifications(memoryPredicate);
    }

    public boolean addNotification(final NotificationFinder notificationFinder,
        final Notification notification) {

      WorkTaskAsync workTask =
          new WorkTaskAsync(NotificationProcessor.this, notificationFinder, env, notification,
              observers);
      FutureTask<?> ft = new FutureNotificationTask(notification, notificationFinder, workTask);

      if (!tracker.add(notification.getRowColumn(), ft)) {
        return false;
      }

      try {
        executor.execute(ft);
      } catch (RejectedExecutionException rje) {
        tracker.remove(notification.getRowColumn());
        throw rje;
      }

      return true;
    }

    public void close() {
      tracker.finishAddingNotifications(id);
    }
  }

  /**
   * Starts a session for adding notifications. During this session, any notifications that are
   * deleted and match the predicate will be remembered. These remembered notifications can not be
   * added again while the session is active.
   */
  public Session beginAddingNotifications(Predicate<RowColumn> memoryPredicate) {
    return new Session(memoryPredicate);
  }

  public void requeueNotification(final NotificationFinder notificationFinder,
      final Notification notification) {

    WorkTaskAsync workTask =
        new WorkTaskAsync(this, notificationFinder, env, notification, observers);
    FutureTask<?> ft = new FutureNotificationTask(notification, notificationFinder, workTask);

    if (tracker.requeue(notification.getRowColumn(), ft)) {
      try {
        executor.execute(ft);
      } catch (RejectedExecutionException rje) {
        tracker.remove(notification.getRowColumn());
        throw rje;
      }
    }
  }

  public void notificationProcessed(final Notification notification) {
    tracker.remove(notification.getRowColumn());
  }

  public int size() {
    return queue.size();
  }

  public void clear() {
    tracker.clear();
    executor.purge();
  }

  @Override
  public void close() {
    executor.shutdownNow();
    observers.close();

    try {
      while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {

      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
