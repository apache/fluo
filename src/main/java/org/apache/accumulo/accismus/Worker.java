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
package org.apache.accumulo.accismus;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.accismus.exceptions.AlreadyAcknowledgedException;
import org.apache.accumulo.accismus.exceptions.CommitException;
import org.apache.accumulo.accismus.impl.ByteUtil;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;

/**
 * A service that looks for updated columns to process
 */
public class Worker {
  private Map<Column,Observer> colObservers = new HashMap<Column,Observer>();
  private Configuration config;
  
  public Worker(Configuration config) throws Exception {

    this.config = config;
    
    Set<Entry<Column,String>> es = config.getObservers().entrySet();
    for (Entry<Column,String> entry : es) {
      Column col = entry.getKey();
      
      colObservers.put(col, Class.forName(entry.getValue()).asSubclass(Observer.class).newInstance());
    }
    
    
  }
  
  private Range pickRandomStartPoint(Scanner scanner) throws TableNotFoundException, AccumuloSecurityException, AccumuloException {
    // TODO use current firstRow + historical max chars seen in each position to compute a random row
    return new Range();
  }

  // TODO make package private
  public void processUpdates() throws Exception {

    Scanner scanner = config.getConnector().createScanner(config.getTable(), config.getAuthorizations());
    scanner.fetchColumnFamily(ByteUtil.toText(Constants.NOTIFY_CF));
    
    scanner.setRange(pickRandomStartPoint(scanner));
    
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
      
      Transaction tx = new Transaction(config, row, col);
      
      while (true)
        try {
          // TODO check ack to see if observer should run
          observer.process(tx, row, col);
          
          tx.commit();
          break;
        } catch (AlreadyAcknowledgedException aae) {
          return;
        } catch (CommitException e) {
          // retry
        }
    }
  }
}
