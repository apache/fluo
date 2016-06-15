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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Supplier;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Range;
import org.apache.fluo.core.impl.Environment;
import org.apache.hadoop.io.Text;

public class TabletInfoCache<T, S extends Supplier<T>> {
  private static final long CACHE_TIME = 5 * 60 * 1000;

  private final Environment env;
  private List<TabletInfo<T>> cachedTablets;
  private long listSplitsTime = 0;
  private S supplier;

  public static class TabletInfo<T> {
    private final Text start;
    private final Text end;
    private T data;

    TabletInfo(Text start, Text end, T data) {
      this.start = start;
      this.end = end;
      this.data = data;
    }

    private int hashCode(Text t) {
      if (t == null) {
        return 0;
      }
      return t.hashCode();
    }

    @Override
    public int hashCode() {
      return hashCode(start) + hashCode(end);
    }

    private boolean equals(Text t1, Text t2) {
      if (t1 == t2) {
        return true;
      }

      if (t1 == null || t2 == null) {
        return false;
      }

      return t1.equals(t2);
    }

    @Override
    public boolean equals(Object o) {
      if (o instanceof TabletInfo) {
        @SuppressWarnings("rawtypes")
        TabletInfo oti = (TabletInfo) o;

        if (equals(start, oti.start)) {
          return equals(end, oti.end);
        }

        return false;
      }

      return false;
    }

    public Text getStart() {
      return start;
    }

    public Text getEnd() {
      return end;
    }

    public T getData() {
      return data;
    }

    public Range getRange() {
      return new Range(start, false, end, true);
    }
  }

  public TabletInfoCache(Environment env, S supplier) {
    this.env = env;
    this.supplier = supplier;
  }

  private List<TabletInfo<T>> listSplits() throws TableNotFoundException,
      AccumuloSecurityException, AccumuloException {
    List<Text> splits =
        new ArrayList<>(env.getConnector().tableOperations().listSplits(env.getTable()));
    Collections.sort(splits);

    List<TabletInfo<T>> tablets = new ArrayList<>(splits.size() + 1);
    for (int i = 0; i < splits.size(); i++) {
      tablets
          .add(new TabletInfo<>(i == 0 ? null : splits.get(i - 1), splits.get(i), supplier.get()));
    }

    tablets.add(new TabletInfo<>(splits.size() == 0 ? null : splits.get(splits.size() - 1), null,
        supplier.get()));
    listSplitsTime = System.currentTimeMillis();
    return tablets;
  }

  public synchronized List<TabletInfo<T>> getTablets() throws Exception {
    if (cachedTablets == null) {
      cachedTablets = listSplits();
    } else if (System.currentTimeMillis() - listSplitsTime > CACHE_TIME) {
      List<TabletInfo<T>> tablets = listSplits();
      Map<TabletInfo<T>, TabletInfo<T>> oldTablets = new HashMap<>();
      for (TabletInfo<T> tabletInfo : cachedTablets) {
        oldTablets.put(tabletInfo, tabletInfo);
      }

      List<TabletInfo<T>> newTablets = new ArrayList<>(tablets.size());

      for (TabletInfo<T> tabletInfo : tablets) {
        TabletInfo<T> oldTI = oldTablets.get(tabletInfo);
        if (oldTI != null) {
          newTablets.add(oldTI);
        } else {
          newTablets.add(tabletInfo);
        }
      }

      cachedTablets = newTablets;
    }

    return Collections.unmodifiableList(cachedTablets);
  }
}
