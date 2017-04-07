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

import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Consumer;

import org.apache.fluo.api.data.Bytes;

public class RangeSet {
  private TreeMap<Bytes, TableRange> tmap;
  private TableRange lastRange;

  public RangeSet(List<TableRange> ranges) {
    tmap = new TreeMap<>();

    for (TableRange tablet : ranges) {
      if (tablet.getEndRow() == null) {
        lastRange = tablet;
      } else {
        tmap.put(tablet.getEndRow(), tablet);
      }
    }
  }

  public TableRange getContaining(Bytes row) {
    Entry<Bytes, TableRange> entry = tmap.ceilingEntry(row);
    if (entry != null) {
      if (entry.getValue().contains(row)) {
        return entry.getValue();
      }
    } else if (lastRange != null) {
      if (lastRange.contains(row)) {
        return lastRange;
      }
    }

    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof RangeSet) {
      RangeSet ots = (RangeSet) o;

      if (tmap.size() != ots.tmap.size()) {
        return false;
      }

      for (Entry<Bytes, TableRange> entry : tmap.entrySet()) {
        TableRange otr = ots.tmap.get(entry.getKey());
        if (!Objects.equals(entry.getValue(), otr)) {
          return false;
        }
      }

      return Objects.equals(lastRange, ots.lastRange);
    }
    return false;
  }

  public int size() {
    return tmap.size() + (lastRange == null ? 0 : 1);
  }

  public void forEach(Consumer<TableRange> trc) {
    if (lastRange != null) {
      trc.accept(lastRange);
    }
    tmap.values().forEach(trc);
  }
}
