/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.accismus.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import org.apache.accumulo.accismus.api.Column;
import org.apache.accumulo.accismus.api.Observer;
import org.apache.accumulo.accismus.api.Transaction;
import org.apache.accumulo.accismus.api.exceptions.AlreadyAcknowledgedException;
import org.apache.accumulo.accismus.api.exceptions.AlreadySetException;
import org.apache.accumulo.accismus.api.exceptions.CommitException;
import org.apache.accumulo.accismus.impl.RandomRowGenerator.DataSource;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A service that looks for updated columns to process
 */
public class Worker {
  
  private static Logger log = Logger.getLogger(Worker.class);

  private Map<Column,Observer> colObservers = new HashMap<Column,Observer>();
  private Configuration config;
  private Random rand = new Random();
  
  public Worker(Configuration config) throws Exception {

    this.config = config;

    Set<Entry<Column,String>> es = config.getObservers().entrySet();
    for (Entry<Column,String> entry : es) {
      Column col = entry.getKey();
      
      Observer observer = Class.forName(entry.getValue()).asSubclass(Observer.class).newInstance();
      colObservers.put(col, observer);
    }
  }
  
  
  private Range pickRandomRow(final Scanner scanner, Text start, Text end) {
    RandomRowGenerator rrg = new RandomRowGenerator(rand);
    
    Text row = rrg.pickRandomRow(new DataSource() {
      
      @Override
      public Iterator<Entry<Key,Value>> scan(Text start, Text end) {
        // TODO set batch size
        scanner.setRange(new Range(start, true, end, true));
        return scanner.iterator();
      }
    }, start, end);

    if (row == null)
      return null;
    
    return new Range(row, true, null, false);
  }
  
  private Range pickRandomStartPoint(Scanner scanner) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    // TODO cache splits for a bit and share between worker threads

    List<Text> splits = new ArrayList<Text>(config.getConnector().tableOperations().listSplits(config.getTable()));

    int num = rand.nextInt(splits.size() + 1);
    
    Text start = num - 1 < 0 ? null : splits.get(num - 1);
    Text end = num >= splits.size() ? null : splits.get(num);
    
    // TODO remember if a tablet is empty an do not retry it for a bit... the more times empty, the longer the retry
    // TODO only have one thread inspecting a tablet for a start location at a time.. want to handle the case w/ few tablets, many workers, and no notifications
    // well
    return pickRandomRow(scanner, start, end);
  }

  // TODO make package private
  public long processUpdates() throws Exception {
    // TODO how does user set auths that workers are expected to use...

    Scanner scanner = config.getConnector().createScanner(config.getTable(), config.getAuthorizations());
    scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));
    
    Range range = pickRandomStartPoint(scanner);
    if (range == null)
      return 0;
    
    scanner.setRange(range);
    
    long numProcessed = 0;

    boolean loggedFirst = false;

    for (Entry<Key,Value> entry : scanner) {
      List<ByteSequence> ca = ByteUtil.split(entry.getKey().getColumnQualifierData());
      Column col = new Column(ca.get(0), ca.get(1));
      // TODO cache col vis
      col.setVisibility(entry.getKey().getColumnVisibilityParsed());
      
      Observer observer = colObservers.get(col);
      if (observer == null) {
        // TODO do something
      }
      
      ByteSequence row = entry.getKey().getRowData();
      
      if (!loggedFirst) {
        log.debug("thread id: " + Thread.currentThread().getId() + "  row :" + row);
        loggedFirst = true;
      }

      while (true)
        try {
          Transaction tx = new TransactionImpl(config, row, col);
          observer.process(tx, row, col);
          tx.commit();
          break;
        } catch (AlreadySetException ase) {
          // this could be caused by multiple worker threads processing the same notification
          scanner.setRange(new Range(entry.getKey(), true, entry.getKey(), true));
          if (scanner.iterator().hasNext()) {
            // notification is still there, so maybe a bug in user code
            throw ase;
          } else {
            return numProcessed;
          }
        } catch (AlreadyAcknowledgedException aae) {
          return numProcessed;
        } catch (CommitException e) {
          // retry
        }
      // TODO if duplicate set detected, see if its because already acknowledged
      
      numProcessed++;
    }
    
    return numProcessed;
  }
}
