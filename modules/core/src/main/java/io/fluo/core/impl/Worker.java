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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import io.fluo.accumulo.iterators.NotificationSampleIterator;
import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.api.client.Transaction;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.RowColumn;
import io.fluo.api.data.Span;
import io.fluo.api.observer.Observer;
import io.fluo.core.exceptions.AlreadyAcknowledgedException;
import io.fluo.core.exceptions.CommitException;
import io.fluo.core.impl.RandomTabletChooser.TabletInfo;
import io.fluo.core.util.ByteUtil;
import io.fluo.core.util.SpanUtil;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.VersioningIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service that looks for updated columns to process
 */
public class Worker {

  // TODO arbitrary
  private static long MAX_SLEEP_TIME = 5 * 60 * 1000;

  private static Logger log = LoggerFactory.getLogger(Worker.class);

  private Environment env;
  private Random rand = new Random();

  private RandomTabletChooser tabletChooser;

  public Worker(Environment env, RandomTabletChooser tabletChooser) throws Exception {
    this.env = env;
    this.tabletChooser = tabletChooser;
  }

  private Span pickRandomRow(final Scanner scanner, Bytes start, Bytes end) {

    scanner.clearScanIterators();
    scanner.clearColumns();

    // table does not have versioning iterator configured, if there are multiple notification versions only want to see one
    IteratorSetting iterCfg = new IteratorSetting(20, "ver", VersioningIterator.class);
    scanner.addScanIterator(iterCfg);

    iterCfg = new IteratorSetting(100, NotificationSampleIterator.class);
    // TODO arbitrary number
    NotificationSampleIterator.setSampleSize(iterCfg, 256);
    scanner.addScanIterator(iterCfg);
    scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));
    scanner.setRange(SpanUtil.toRange(new Span(start, false, end, true)));

    ArrayList<Bytes> sample = new ArrayList<>();

    for (Entry<Key,Value> entry : scanner)
      sample.add(ByteUtil.toBytes(entry.getKey().getRow()));

    if (sample.size() == 0)
      return null;

    Bytes row = sample.get(rand.nextInt(sample.size()));

    return new Span(row, true, end, true);
  }

  private Span pickRandomStartPoint(Scanner scanner) throws Exception {

    TabletInfo tablet = tabletChooser.getRandomTablet();
    // only have one thread per process inspecting a tablet for a start location at a time.. want to handle the case w/ few tablets, many workers, and no
    // notifications well
    if (tablet != null) {
      try {
        if (tablet.retryTime > System.currentTimeMillis()) {
          return null;
        }
                
        Bytes start = tablet.start == null ? Bytes.EMPTY : ByteUtil.toBytes(tablet.start);
        Bytes end = tablet.end == null ? Bytes.EMPTY : ByteUtil.toBytes(tablet.end);

        Span ret = pickRandomRow(scanner, start, end);
        if (ret == null) {
          // remember if a tablet is empty an do not retry it for a bit... the more times empty, the longer the retry
          tablet.retryTime = tablet.sleepTime + System.currentTimeMillis();
          if (tablet.sleepTime < MAX_SLEEP_TIME)
            tablet.sleepTime = tablet.sleepTime + (long) (tablet.sleepTime * Math.random());
        } else {
          tablet.retryTime = 0;
          tablet.sleepTime = 0;
        }

        return ret;
      } finally {
        tablet.lock.unlock();
      }
    } else {
      return null;
    }
  }

  // TODO make package private
  public long processUpdates(Map<Column,Observer> colObservers) throws Exception {
    // TODO how does user set auths that workers are expected to use...

    Scanner scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());

    Span span = pickRandomStartPoint(scanner);
    if (span == null)
      return 0;

    scanner.clearColumns();
    scanner.clearScanIterators();

    // table does not have versioning iterator configured, if there are multiple notification versions only want to see one
    IteratorSetting iterCfg = new IteratorSetting(20, "ver", VersioningIterator.class);
    scanner.addScanIterator(iterCfg);

    scanner.fetchColumnFamily(ByteUtil.toText(ColumnConstants.NOTIFY_CF));
    scanner.setRange(SpanUtil.toRange(span));

    long numProcessed = 0;

    boolean loggedFirst = false;

    for (Entry<Key,Value> entry : scanner) {
      List<Bytes> ca = Bytes.split(Bytes.wrap(entry.getKey().getColumnQualifierData().toArray()));
      Column col = new Column(ca.get(0), ca.get(1));
      // TODO cache col vis
      col.setVisibility(entry.getKey().getColumnVisibilityParsed());

      Observer observer = getObserver(colObservers, col);

      Bytes row = Bytes.wrap(entry.getKey().getRowData().toArray());

      if (!loggedFirst) {
        log.debug("thread id: " + Thread.currentThread().getId() + "  row :" + row);
        loggedFirst = true;
      }

      TransactionImpl txi = null;
      try {
        while (true) {
          String status = "FAILED";
          try {
            txi = new TransactionImpl(env, row, col);
            Transaction tx = txi;
            if (TracingTransaction.isTracingEnabled())
              tx = new TracingTransaction(tx);

            observer.process(tx, row, col);
            txi.commit();
            status = "COMMITTED";
            break;
          } catch (AlreadyAcknowledgedException aae) {
            status = "AACKED";
            return numProcessed;
          } catch (CommitException e) {
            // retry
          } catch (Exception e) {
            // this could be caused by multiple worker threads processing the same notification
            // TODO this detection method has a race condition, notification could be recreated after being deleted... need to check notification timestamp
            RowColumn rc = SpanUtil.toRowColumn(entry.getKey());
            scanner.setRange(SpanUtil.toRange(new Span(rc, true, rc, true)));
            if (scanner.iterator().hasNext()) {
              // notification is still there, so maybe a bug in user code
              throw e;
            } else {
              // no notification, so maybe another thread processed notification
              log.debug("Failure processing notification concurrently ", e);
              return numProcessed;
            }
          } finally {
            if (txi != null && TxLogger.isLoggingEnabled())
              TxLogger.logTx(status, observer.getClass().getSimpleName(), txi.getStats(), row + ":" + col);
          }
          // TODO if duplicate set detected, see if its because already acknowledged
        }
      } finally {
        // close after multiple commit attempts
        if (txi != null) {
          txi.close();
        }
      }
      numProcessed++;
    }

    return numProcessed;
  }

  private Observer getObserver(Map<Column,Observer> colObservers, Column col) throws Exception {
    Observer observer = colObservers.get(col);
    if (observer == null) {
      ObserverConfiguration observerConfig = env.getObservers().get(col);
      if (observerConfig == null)
        observerConfig = env.getWeakObservers().get(col);

      if (observerConfig != null) {
        observer = Class.forName(observerConfig.getClassName()).asSubclass(Observer.class).newInstance();
        observer.init(observerConfig.getParameters());

        if (!observer.getObservedColumn().getColumn().equals(col)) {
          throw new IllegalStateException("Mismatch between configured column and class column " + observerConfig.getClassName() + " " + col + " "
              + observer.getObservedColumn().getColumn());
        }

        colObservers.put(col, observer);
      }
      // TODO do something
    }
    return observer;
  }
}
