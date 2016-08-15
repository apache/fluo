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

package org.apache.fluo.integration.log;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.apache.fluo.api.client.Loader;
import org.apache.fluo.api.client.LoaderExecutor;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.Transaction;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.ObserverConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.observer.AbstractObserver;
import org.apache.fluo.integration.ITBaseMini;
import org.apache.fluo.integration.TestUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Test;

public class LogIT extends ITBaseMini {

  private static final Column STAT_COUNT = new Column("stat", "count");

  static class SimpleLoader implements Loader {

    public void load(TransactionBase tx, Context context) throws Exception {
      TestUtil.increment(tx, "r1", new Column("a", "b"), 1);
    }
  }

  static class TriggerLoader implements Loader {

    int r;

    TriggerLoader(int row) {
      r = row;
    }

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
      tx.set(r + "", STAT_COUNT, "1");
      tx.setWeakNotification(r + "", STAT_COUNT);
    }
  }

  private static Bytes bRow1 = Bytes.of(new byte[] {'r', 0x05, '1'});
  private static Bytes bRow2 = Bytes.of(new byte[] {'r', 0x06, '2'});

  private static Column bCol1 = new Column(Bytes.of(new byte[] {'c', 0x02, '1'}),
      Bytes.of(new byte[] {'c', (byte) 0xf5, '1'}));
  private static Column bCol2 = new Column(Bytes.of(new byte[] {'c', 0x09, '2'}),
      Bytes.of(new byte[] {'c', (byte) 0xe5, '2'}));

  static class BinaryLoader1 implements Loader {

    @Override
    public void load(TransactionBase tx, Context context) throws Exception {
      tx.delete(bRow1, bCol1);
      tx.get(bRow2, bCol1);

      tx.get(bRow2, ImmutableSet.of(bCol1, bCol2));
      tx.get(ImmutableSet.of(bRow1, bRow2), ImmutableSet.of(bCol1, bCol2));

      tx.set(bRow1, bCol2, Bytes.of(new byte[] {'v', (byte) 0x99, '2'}));
      tx.set(bRow2, bCol2, Bytes.of(new byte[] {'v', (byte) 0xd9, '1'}));

      tx.setWeakNotification(bRow2, bCol2);
    }
  }

  public static class BinaryObserver extends AbstractObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(bCol2, NotificationType.WEAK);
    }

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) {
      tx.get(bRow1, bCol2);
      tx.get(bRow2, ImmutableSet.of(bCol1, bCol2));
      tx.get(ImmutableSet.of(bRow1, bRow2), ImmutableSet.of(bCol1, bCol2));
    }
  }

  public static class TestObserver extends AbstractObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(STAT_COUNT, NotificationType.WEAK);
    }

    @Override
    public void process(TransactionBase tx, Bytes row, Column col) {
      TestUtil.increment(tx, "all", col, Integer.parseInt(tx.gets(row.toString(), col)));
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Arrays.asList(new ObserverConfiguration(TestObserver.class.getName()),
        new ObserverConfiguration(BinaryObserver.class.getName()));
  }

  @Test
  public void testCollisionLogging() throws Exception {
    Logger logger = Logger.getLogger("fluo.tx.collisions");

    StringWriter writer = new StringWriter();
    WriterAppender appender = new WriterAppender(new PatternLayout("%p, %m%n"), writer);

    Level level = logger.getLevel();
    boolean additivity = logger.getAdditivity();
    try {
      logger.setLevel(Level.TRACE);
      logger.setAdditivity(false);
      logger.addAppender(appender);

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        for (int i = 0; i < 20; i++) {
          le.execute(new SimpleLoader());
          le.execute(new TriggerLoader(i));
        }
      }

      miniFluo.waitForObservers();
    } finally {
      logger.removeAppender(appender);
      logger.setAdditivity(additivity);
      logger.setLevel(level);
    }

    String logMsgs = writer.toString();
    logMsgs = logMsgs.replace('\n', ' ');

    Assert.assertFalse(logMsgs.contains("TriggerLoader"));

    String pattern;

    pattern = ".*txid: (\\d+) class: org.apache.fluo.integration.log.LogIT\\$SimpleLoader";
    pattern += ".*txid: \\1 collisions: \\Q{r1=[a b ]}\\E.*";
    Assert.assertTrue(logMsgs.matches(pattern));

    pattern = ".*txid: (\\d+) trigger: \\d+ stat count  \\d+";
    pattern += ".*txid: \\1 class: org.apache.fluo.integration.log.LogIT\\$TestObserver";
    pattern += ".*txid: \\1 collisions: \\Q{all=[stat count ]}\\E.*";
    Assert.assertTrue(logMsgs.matches(pattern));
  }

  @Test
  public void testSummaryLogging() throws Exception {
    Logger logger = Logger.getLogger("fluo.tx.summary");

    StringWriter writer = new StringWriter();
    WriterAppender appender = new WriterAppender(new PatternLayout("%p, %m%n"), writer);

    Level level = logger.getLevel();
    boolean additivity = logger.getAdditivity();

    try {
      logger.setLevel(Level.TRACE);
      logger.setAdditivity(false);
      logger.addAppender(appender);

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        for (int i = 0; i < 20; i++) {
          le.execute(new SimpleLoader());
          le.execute(new TriggerLoader(i));
        }
      }

      miniFluo.waitForObservers();
    } finally {
      logger.removeAppender(appender);
      logger.setAdditivity(additivity);
      logger.setLevel(level);
    }

    String logMsgs = writer.toString();
    logMsgs = logMsgs.replace('\n', ' ');

    Assert
        .assertTrue(logMsgs
            .matches(".*txid: \\d+ thread : \\d+ time: \\d+ \\(\\d+ \\d+\\) #ret: 0 #set: 1 #collisions: 0 waitTime: \\d+ committed: true class: TriggerLoader.*"));
    Assert
        .assertTrue(logMsgs
            .matches(".*txid: \\d+ thread : \\d+ time: \\d+ \\(\\d+ \\d+\\) #ret: 1 #set: 1 #collisions: 0 waitTime: \\d+ committed: true class: SimpleLoader.*"));
    Assert
        .assertTrue(logMsgs
            .matches(".*txid: \\d+ thread : \\d+ time: \\d+ \\(\\d+ \\d+\\) #ret: 1 #set: 1 #collisions: 1 waitTime: \\d+ committed: false class: SimpleLoader.*"));
    Assert
        .assertTrue(logMsgs
            .matches(".*txid: \\d+ thread : \\d+ time: \\d+ \\(\\d+ \\d+\\) #ret: 2 #set: 1 #collisions: 0 waitTime: \\d+ committed: true class: TestObserver.*"));
    Assert
        .assertTrue(logMsgs
            .matches(".*txid: \\d+ thread : \\d+ time: \\d+ \\(\\d+ \\d+\\) #ret: 2 #set: 1 #collisions: 1 waitTime: \\d+ committed: false class: TestObserver.*"));
  }

  @Test
  public void testAllLogging() throws Exception {
    Logger logger = Logger.getLogger("fluo.tx");

    StringWriter writer = new StringWriter();
    WriterAppender appender =
        new WriterAppender(new PatternLayout("%d{ISO8601} [%-8c{2}] %-5p: %m%n"), writer);

    Level level = logger.getLevel();
    boolean additivity = logger.getAdditivity();

    try {
      logger.setLevel(Level.TRACE);
      logger.setAdditivity(false);
      logger.addAppender(appender);

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(new SimpleLoader());
      }

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(new TriggerLoader(0));
      }
      miniFluo.waitForObservers();

      try (Snapshot snap = client.newSnapshot()) {
        Assert.assertTrue(Integer.parseInt(snap.gets("all", STAT_COUNT)) >= 1);
        Assert.assertEquals("1", snap.gets("r1", new Column("a", "b")));
      }
    } finally {
      logger.removeAppender(appender);
      logger.setAdditivity(additivity);
      logger.setLevel(level);
    }

    String logMsgs = writer.toString();
    logMsgs = logMsgs.replace('\n', ' ');

    String pattern;

    // simple loader should cause this pattern in logs
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 class: org.apache.fluo.integration.log.LogIT\\$SimpleLoader";
    pattern += ".*txid: \\1 get\\(r1, a b \\) -> null";
    pattern += ".*txid: \\1 set\\(r1, a b , 1\\)";
    pattern += ".*txid: \\1 commit\\(\\) -> SUCCESSFUL commitTs: \\d+";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));

    // trigger loader should cause this pattern in logs
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 class: org.apache.fluo.integration.log.LogIT\\$TriggerLoader";
    pattern += ".*txid: \\1 set\\(0, stat count , 1\\)";
    pattern += ".*txid: \\1 setWeakNotification\\(0, stat count \\)";
    pattern += ".*txid: \\1 commit\\(\\) -> SUCCESSFUL commitTs: \\d+";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));

    // observer should cause this pattern in logs
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 trigger: 0 stat count  \\d+";
    pattern += ".*txid: \\1 class: org.apache.fluo.integration.log.LogIT\\$TestObserver";
    pattern += ".*txid: \\1 get\\(0, stat count \\) -> 1";
    pattern += ".*txid: \\1 get\\(all, stat count \\) -> null";
    pattern += ".*txid: \\1 set\\(all, stat count , 1\\)";
    pattern += ".*txid: \\1 commit\\(\\) -> SUCCESSFUL commitTs: \\d+";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));

    // two gets done by snapshot should cause this pattern
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 get\\(all, stat count \\) -> 1";
    pattern += ".*txid: \\1 get\\(r1, a b \\) -> 1";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));
  }

  @Test
  public void testGetMethods() {

    Column c1 = new Column("f1", "q1");
    Column c2 = new Column("f1", "q2");

    try (Transaction tx = client.newTransaction()) {
      tx.set("r1", c1, "v1");
      tx.set("r1", c2, "v2");
      tx.set("r2", c1, "v3");
      tx.set("r2", c2, "v4");
      tx.commit();
    }


    Logger logger = Logger.getLogger("fluo.tx");

    StringWriter writer = new StringWriter();
    WriterAppender appender =
        new WriterAppender(new PatternLayout("%d{ISO8601} [%-8c{2}] %-5p: %m%n"), writer);

    Level level = logger.getLevel();
    boolean additivity = logger.getAdditivity();

    try {
      logger.setLevel(Level.TRACE);
      logger.setAdditivity(false);
      logger.addAppender(appender);

      try (Snapshot snap = client.newSnapshot()) {
        snap.gets(Arrays.asList(new RowColumn("r1", c1), new RowColumn("r2", c2)));
        snap.gets(Arrays.asList("r1", "r2"), ImmutableSet.of(c1));
        snap.gets("r1", ImmutableSet.of(c1, c2));
        snap.gets("r1", c1);
      }

      miniFluo.waitForObservers();
    } finally {
      logger.removeAppender(appender);
      logger.setAdditivity(additivity);
      logger.setLevel(level);
    }

    String origLogMsgs = writer.toString();
    String logMsgs = origLogMsgs.replace('\n', ' ');

    String pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 \\Qget([r1 f1 q1 , r2 f1 q2 ]) -> [r2 f1 q2 =v4, r1 f1 q1 =v1]\\E";
    pattern += ".*txid: \\1 \\Qget([r1, r2], [f1 q1 ]) -> [r1=[f1 q1 =v1], r2=[f1 q1 =v3]]\\E";
    pattern += ".*txid: \\1 \\Qget(r1, [f1 q1 , f1 q2 ]) -> [f1 q1 =v1, f1 q2 =v2]\\E";
    pattern += ".*txid: \\1 \\Qget(r1, f1 q1 ) -> v1\\E";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));
  }

  @Test
  public void testBinaryLogging() throws Exception {
    Logger logger = Logger.getLogger("fluo.tx");

    StringWriter writer = new StringWriter();
    WriterAppender appender =
        new WriterAppender(new PatternLayout("%d{ISO8601} [%-8c{2}] %-5p: %m%n"), writer);

    Level level = logger.getLevel();
    boolean additivity = logger.getAdditivity();

    try {
      logger.setLevel(Level.TRACE);
      logger.setAdditivity(false);
      logger.addAppender(appender);

      try (LoaderExecutor le = client.newLoaderExecutor()) {
        le.execute(new BinaryLoader1());
      }

      miniFluo.waitForObservers();
    } finally {
      logger.removeAppender(appender);
      logger.setAdditivity(additivity);
      logger.setLevel(level);
    }

    String origLogMsgs = writer.toString();
    String logMsgs = origLogMsgs.replace('\n', ' ');

    String pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 class: org.apache.fluo.integration.log.LogIT\\$BinaryLoader1";
    pattern += ".*txid: \\1 \\Qdelete(r\\x051, c\\x021 c\\xf51 )\\E";
    pattern += ".*txid: \\1 \\Qget(r\\x062, c\\x021 c\\xf51 ) -> null\\E";
    pattern += ".*txid: \\1 \\Qget(r\\x062, [c\\x021 c\\xf51 , c\\x092 c\\xe52 ]) -> []\\E";
    pattern +=
        ".*txid: \\1 \\Qget([r\\x051, r\\x062], [c\\x021 c\\xf51 , c\\x092 c\\xe52 ]) -> []\\E";
    pattern += ".*txid: \\1 \\Qset(r\\x051, c\\x092 c\\xe52 , v\\x992)\\E";
    pattern += ".*txid: \\1 \\Qset(r\\x062, c\\x092 c\\xe52 , v\\xd91)\\E";
    pattern += ".*txid: \\1 \\QsetWeakNotification(r\\x062, c\\x092 c\\xe52 )\\E";
    pattern += ".*txid: \\1 \\Qcommit()\\E -> SUCCESSFUL commitTs: \\d+";
    pattern += ".*txid: \\1 \\Qclose()\\E";
    pattern += ".*";
    Assert.assertTrue(origLogMsgs, logMsgs.matches(pattern));

    String v1 = "\\Qr\\x051=[c\\x092 c\\xe52 =v\\x992]\\E";
    String v2 = "\\Qr\\x062=[c\\x092 c\\xe52 =v\\xd91]\\E";

    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 \\Qtrigger: r\\x062 c\\x092 c\\xe52  4\\E";
    pattern += ".*txid: \\1 \\Qclass: org.apache.fluo.integration.log.LogIT$BinaryObserver\\E";
    pattern += ".*txid: \\1 \\Qget(r\\x051, c\\x092 c\\xe52 ) -> v\\x992\\E";
    pattern +=
        ".*txid: \\1 \\Qget(r\\x062, [c\\x021 c\\xf51 , c\\x092 c\\xe52 ]) -> [c\\x092 c\\xe52 =v\\xd91]\\E";
    pattern +=
        ".*txid: \\1 \\Qget([r\\x051, r\\x062], [c\\x021 c\\xf51 , c\\x092 c\\xe52 ]) -> [\\E("
            + v1 + "|" + v2 + ")\\, (" + v1 + "|" + v2 + ")\\]";
    pattern += ".*txid: \\1 \\Qcommit() -> SUCCESSFUL commitTs: -1\\E";
    pattern += ".*txid: \\1 \\Qclose()\\E";
    pattern += ".*";
    Assert.assertTrue(origLogMsgs, logMsgs.matches(pattern));
  }
}
