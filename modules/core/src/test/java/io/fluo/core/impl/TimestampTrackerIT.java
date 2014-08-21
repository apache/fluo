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
package io.fluo.core.impl;

import java.util.NoSuchElementException;

import io.fluo.accumulo.util.LongUtil;
import io.fluo.accumulo.util.ZookeeperUtil;
import io.fluo.core.TestBaseImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests TimestampTracker class
 */
public class TimestampTrackerIT extends TestBaseImpl {

  @Test(expected = NoSuchElementException.class)
  public void testTsNoElement() {
    TimestampTracker tracker = env.getSharedResources().getTimestampTracker();
    Assert.assertTrue(tracker.isEmpty());
    tracker.getOldestActiveTimestamp();
  }

  @Test
  public void testTrackingWithNoUpdate() throws Exception {
    TimestampTracker tracker = new TimestampTracker(env, new TransactorID(env));
    Assert.assertTrue(tracker.isEmpty());
    Assert.assertFalse(zkNodeExists(tracker));
    long ts1 = tracker.allocateTimestamp();
    Assert.assertFalse(tracker.isEmpty());
    Assert.assertTrue(zkNodeExists(tracker));
    Assert.assertTrue(ts1 > zkNodeValue(tracker));
    Assert.assertEquals(tracker.getZookeeperTimestamp(), zkNodeValue(tracker));
    Assert.assertEquals(ts1, tracker.getOldestActiveTimestamp());
    long ts2 = tracker.allocateTimestamp();
    Assert.assertEquals(ts1, tracker.getOldestActiveTimestamp());
    tracker.removeTimestamp(ts1);
    Assert.assertFalse(tracker.isEmpty());
    Assert.assertEquals(ts2, tracker.getOldestActiveTimestamp());
    Assert.assertFalse(tracker.isEmpty());
    Assert.assertTrue(ts1 > zkNodeValue(tracker));
    Assert.assertEquals(tracker.getZookeeperTimestamp(), zkNodeValue(tracker));
    tracker.removeTimestamp(ts2);
    Assert.assertTrue(tracker.isEmpty());
    Assert.assertTrue(zkNodeExists(tracker));
  }

  @Test
  public void testTrackingWithZkUpdate() throws Exception {
    TimestampTracker tracker = new TimestampTracker(env, new TransactorID(env), 5);
    long ts1 = tracker.allocateTimestamp();
    Thread.sleep(15);
    Assert.assertNotNull(ts1);
    Assert.assertTrue(zkNodeExists(tracker));
    Assert.assertNotNull(zkNodeValue(tracker));
    Assert.assertEquals(tracker.getZookeeperTimestamp(), zkNodeValue(tracker));
    Assert.assertEquals(ts1, tracker.getOldestActiveTimestamp());
    long ts2 = tracker.allocateTimestamp();
    Assert.assertEquals(ts1, tracker.getOldestActiveTimestamp());
    Thread.sleep(15);
    tracker.removeTimestamp(ts1);
    Thread.sleep(15);
    Assert.assertEquals(ts2, tracker.getOldestActiveTimestamp());
    Assert.assertEquals(ts2, zkNodeValue(tracker));
    tracker.removeTimestamp(ts2);
    Thread.sleep(15);
    Assert.assertTrue(tracker.isEmpty());
    Assert.assertFalse(zkNodeExists(tracker));
  }

  @Test
  public void testTimestampUtilGetOldestTs() throws InterruptedException {
    Assert.assertEquals(0, getOldestTs());
    TimestampTracker tr1 = new TimestampTracker(env, new TransactorID(env), 5);
    long ts1 = tr1.allocateTimestamp();
    Thread.sleep(15);
    Assert.assertEquals(tr1.getZookeeperTimestamp(), getOldestTs());
    TimestampTracker tr2 = new TimestampTracker(env, new TransactorID(env), 5);
    long ts2 = tr2.allocateTimestamp();
    TimestampTracker tr3 = new TimestampTracker(env, new TransactorID(env), 5);
    long ts3 = tr3.allocateTimestamp();
    Thread.sleep(15);
    Assert.assertEquals(ts1, getOldestTs());
    tr1.removeTimestamp(ts1);
    Thread.sleep(15);
    Assert.assertEquals(ts2, getOldestTs());
    tr2.removeTimestamp(ts2);
    Thread.sleep(15);
    Assert.assertEquals(ts3, getOldestTs());
    tr3.removeTimestamp(ts3);
    tr1.close();
    tr2.close();
    tr3.close();
  }

  private long getOldestTs() {
    return ZookeeperUtil.getOldestTimestamp(env.getZookeepers(), env.getZookeeperRoot());
  }

  private boolean zkNodeExists(TimestampTracker tracker) throws Exception {
    return curator.checkExists().forPath(tracker.getNodePath()) != null;
  }

  private long zkNodeValue(TimestampTracker tracker) throws Exception {
    if (zkNodeExists(tracker) == false) {
      throw new IllegalStateException("node does not exist");
    }
    return LongUtil.fromByteArray(curator.getData().forPath(tracker.getNodePath()));
  }
}
