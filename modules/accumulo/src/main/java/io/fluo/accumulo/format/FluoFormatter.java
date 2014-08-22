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
package io.fluo.accumulo.format;

import java.util.Iterator;
import java.util.Map.Entry;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.DelLockValue;
import io.fluo.accumulo.values.LockValue;
import io.fluo.accumulo.values.WriteValue;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;

/**
 * Converts Accumulo table data to a human-readable Fluo format 
 */
public class FluoFormatter implements Formatter {
  
  private Iterator<Entry<Key,Value>> scanner;
  
  public boolean hasNext() {
    return scanner.hasNext();
  }
  
  public String next() {
    Entry<Key,Value> entry = scanner.next();
    return toString(entry);
  }

  public static String toString(Entry<Key,Value> entry) {
    Key key = entry.getKey();
    
    long ts = key.getTimestamp();
    String type = "";
    
    if ((ts & ColumnConstants.PREFIX_MASK) == ColumnConstants.TX_DONE_PREFIX)
      type = "TX_DONE";
    if ((ts & ColumnConstants.PREFIX_MASK) == ColumnConstants.DEL_LOCK_PREFIX)
      type = "DEL_LOCK";
    if ((ts & ColumnConstants.PREFIX_MASK) == ColumnConstants.LOCK_PREFIX)
      type = "LOCK";
    if ((ts & ColumnConstants.PREFIX_MASK) == ColumnConstants.DATA_PREFIX)
      type = "DATA";
    if ((ts & ColumnConstants.PREFIX_MASK) == ColumnConstants.WRITE_PREFIX)
      type = "WRITE";
    if ((ts & ColumnConstants.PREFIX_MASK) == ColumnConstants.ACK_PREFIX)
      type = "ACK";
    
    String val;
    if (type.equals("WRITE")) {
      val = new WriteValue(entry.getValue().get()).toString();
    } else if (type.equals("DEL_LOCK")) {
      val = new DelLockValue(entry.getValue().get()).toString();
    } else if (type.equals("LOCK")) {
      // TODO can Value be made to extend Bytes w/o breaking API?
      val = new LockValue(entry.getValue().get()).toString();
    } else {
      val = entry.getValue().toString();
    }
    
    return key.getRow() + " " + key.getColumnFamily() + ":" + key.getColumnQualifier() + " [" + key.getColumnVisibility() + "] "
        + (ts & ColumnConstants.TIMESTAMP_MASK) + "-" + type + "\t" + val;
  }

  public void remove() {
    scanner.remove();
  }
  
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    this.scanner = scanner.iterator();
  }
  
}
