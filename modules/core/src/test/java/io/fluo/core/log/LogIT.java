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

package io.fluo.core.log;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import io.fluo.api.types.TypedLoader;
import io.fluo.api.types.TypedObserver;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.api.types.TypedTransactionBase;
import io.fluo.core.ITBaseMini;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.WriterAppender;
import org.junit.Assert;
import org.junit.Test;

public class LogIT extends ITBaseMini {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  static class SimpleLoader extends TypedLoader {
    @Override
    public void load(TypedTransactionBase tx, Context context) throws Exception {
      tx.mutate().row("r1").fam("a").qual("b").increment(1);
    }
  }

  static class TriggerLoader extends TypedLoader {

    int r;

    TriggerLoader(int row) {
      r = row;
    }

    @Override
    public void load(TypedTransactionBase tx, Context context) throws Exception {
      tx.mutate().row(r).fam("stat").qual("count").set(1);
      tx.mutate().row(r).fam("stat").qual("count").weaklyNotify();
    }
  }

  public static class TestObserver extends TypedObserver {

    @Override
    public ObservedColumn getObservedColumn() {
      return new ObservedColumn(tl.bc().fam("stat").qual("count").vis(), NotificationType.WEAK);
    }

    @Override
    public void process(TypedTransactionBase tx, Bytes row, Column col) {
      tx.mutate().row("all").col(col).increment(tx.get().row(row).col(col).toInteger());
    }
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Arrays.asList(new ObserverConfiguration(TestObserver.class.getName()));
  }

  @Test
  public void testCollisionLogging() throws Exception {
    Logger logger = Logger.getLogger("io.fluo.tx.collisions");

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

    pattern = ".*txid: (\\d+) class: io.fluo.core.log.LogIT\\$SimpleLoader";
    pattern += ".*txid: \\1 collisions: \\Q{r1=[a b ]}\\E.*";
    Assert.assertTrue(logMsgs.matches(pattern));

    pattern = ".*txid: (\\d+) trigger: \\d+ stat count";
    pattern += ".*txid: \\1 class: io.fluo.core.log.LogIT\\$TestObserver";
    pattern += ".*txid: \\1 collisions: \\Q{all=[stat count ]}\\E.*";
    Assert.assertTrue(logMsgs.matches(pattern));
  }

  @Test
  public void testSummaryLogging() throws Exception {
    Logger logger = Logger.getLogger("io.fluo.tx.summary");

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

    String logMsgs = writer.toString().replace('\n', ' ');
    
    Assert.assertTrue(logMsgs
        .matches(".*txid: \\d+ thread : \\d+ time: \\d+ #ret: 0 #set: 1 #collisions: 0 waitTime: \\d+ %s committed: true class: TriggerLoader.*"));
    Assert.assertTrue(logMsgs
        .matches(".*txid: \\d+ thread : \\d+ time: \\d+ #ret: 1 #set: 1 #collisions: 0 waitTime: \\d+ %s committed: true class: SimpleLoader.*"));
    Assert.assertTrue(logMsgs
        .matches(".*txid: \\d+ thread : \\d+ time: \\d+ #ret: 1 #set: 1 #collisions: 1 waitTime: \\d+ %s committed: false class: SimpleLoader.*"));
    Assert.assertTrue(logMsgs
        .matches(".*txid: \\d+ thread : \\d+ time: \\d+ #ret: 2 #set: 2 #collisions: 0 waitTime: \\d+ %s committed: true class: TestObserver.*"));
    Assert.assertTrue(logMsgs
        .matches(".*txid: \\d+ thread : \\d+ time: \\d+ #ret: 2 #set: 2 #collisions: 1 waitTime: \\d+ %s committed: false class: TestObserver.*"));
  }
  
  @Test
  public void testAllLogging() throws Exception {
    Logger logger = Logger.getLogger("io.fluo.tx");

    StringWriter writer = new StringWriter();
    WriterAppender appender = new WriterAppender(new PatternLayout("%d{ISO8601} [%-8c{2}] %-5p: %m%n"), writer);

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

      try(TypedSnapshot snap = tl.wrap(client.newSnapshot())){
        Assert.assertEquals(1, snap.get().row("all").fam("stat").qual("count").toInteger(-1));
        Assert.assertEquals(1, snap.get().row("r1").fam("a").qual("b").toInteger(-1));
      }
    } finally {
      logger.removeAppender(appender);
      logger.setAdditivity(additivity);
      logger.setLevel(level);
    }

    String logMsgs = writer.toString();
    System.out.println(logMsgs);
    logMsgs = logMsgs.replace('\n', ' ');
    
    String pattern;
    
    //simple loader should cause this pattern in logs
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 class: io.fluo.core.log.LogIT\\$SimpleLoader";
    pattern += ".*txid: \\1 get\\(r1, a b \\) -> null";
    pattern += ".*txid: \\1 set\\(r1, a b , 1\\)";
    pattern += ".*txid: \\1 commit\\(\\) -> SUCCESSFUL commitTs: \\d+";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));
    
    //trigger loader should cause this pattern in logs
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 class: io.fluo.core.log.LogIT\\$TriggerLoader";
    pattern += ".*txid: \\1 set\\(0, stat count , 1\\)";
    pattern += ".*txid: \\1 setWeakNotification\\(0, stat count \\)";
    pattern += ".*txid: \\1 commit\\(\\) -> SUCCESSFUL commitTs: \\d+";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));
    
    //observer should cause this pattern in logs
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+";
    pattern += ".*txid: \\1 trigger: 0 stat count"; 
    pattern += ".*txid: \\1 class: io.fluo.core.log.LogIT\\$TestObserver";
    pattern += ".*txid: \\1 get\\(0, stat count \\) -> 1";
    pattern += ".*txid: \\1 get\\(all, stat count \\) -> null";
    pattern += ".*txid: \\1 set\\(all, stat count , 1\\)";
    pattern += ".*txid: \\1 commit\\(\\) -> SUCCESSFUL commitTs: 9";
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));
    
    //two gets done by snapshot should cause this pattern
    pattern = ".*txid: (\\d+) begin\\(\\) thread: \\d+"; 
    pattern += ".*txid: \\1 get\\(all, stat count \\) -> 1"; 
    pattern += ".*txid: \\1 get\\(r1, a b \\) -> 1"; 
    pattern += ".*txid: \\1 close\\(\\).*";
    Assert.assertTrue(logMsgs.matches(pattern));
  }
}
