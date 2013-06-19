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
package org.apache.accumulo.accismus.format;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.accismus.impl.ColumnUtil;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.util.format.Formatter;

/**
 * 
 */
public class AccismusFormatter implements Formatter {
  
  private Iterator<Entry<Key,Value>> scanner;
  
  public boolean hasNext() {
    return scanner.hasNext();
  }
  
  public String next() {
    Entry<Key,Value> entry = scanner.next();
    
    Key key = entry.getKey();
    
    long ts = key.getTimestamp();
    String type = "";
    
    if ((ts & ColumnUtil.PREFIX_MASK) == ColumnUtil.DEL_LOCK_PREFIX)
      type = "DEL_LOCK";
    if ((ts & ColumnUtil.PREFIX_MASK) == ColumnUtil.LOCK_PREFIX)
      type = "LOCK";
    if ((ts & ColumnUtil.PREFIX_MASK) == ColumnUtil.DATA_PREFIX)
      type = "DATA";
    if ((ts & ColumnUtil.PREFIX_MASK) == ColumnUtil.WRITE_PREFIX)
      type = "WRITE";
    if ((ts & ColumnUtil.PREFIX_MASK) == ColumnUtil.ACK_PREFIX)
      type = "ACK";
    
    String val;
    if (type.equals("WRITE")) {
      byte ba[] = entry.getValue().get();
      if (ba.length == 1) {
        val = new String(ba);
      } else {
        val = ((((long) ba[0] << 56) + ((long) (ba[1] & 255) << 48) + ((long) (ba[2] & 255) << 40) + ((long) (ba[3] & 255) << 32)
            + ((long) (ba[4] & 255) << 24) + ((ba[5] & 255) << 16) + ((ba[6] & 255) << 8) + ((ba[7] & 255) << 0)))
            + "";
      }
    } else {
      val = entry.getValue().toString();
    }
    
    return key.getRow() + " " + key.getColumnFamily() + " " + key.getColumnQualifier() + " " + key.getColumnVisibility() + " " + type + " "
        + (ts & ColumnUtil.TIMESTAMP_MASK) + " " + val;
    
  }
  
  public void remove() {
    scanner.remove();
  }
  
  public void initialize(Iterable<Entry<Key,Value>> scanner, boolean printTimestamps) {
    this.scanner = scanner.iterator();
  }
  
}
