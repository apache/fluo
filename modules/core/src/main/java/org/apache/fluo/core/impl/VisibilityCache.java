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

package org.apache.fluo.core.impl;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.core.util.ByteUtil;

/**
 * PArsing Column visibilities can be expensive. This class provides a cache of parsed visibility
 * objects.
 */

public class VisibilityCache {

  public static final ColumnVisibility EMPTY_VIS = new ColumnVisibility();

  private static class VisWeigher implements Weigher<Bytes, ColumnVisibility> {
    @Override
    public int weigh(Bytes key, ColumnVisibility vis) {
      return key.length() + vis.getExpression().length + 32;
    }
  }

  private final Cache<Bytes, ColumnVisibility> visCache;

  VisibilityCache() {
    visCache =
        CacheBuilder.newBuilder().expireAfterAccess(TxInfoCache.CACHE_TIMEOUT_MIN, TimeUnit.MINUTES)
            .maximumWeight(10000000).weigher(new VisWeigher()).concurrencyLevel(10).build();
  }

  public ColumnVisibility getCV(Column col) {
    return getCV(col.getVisibility());
  }

  public ColumnVisibility getCV(final Bytes colvis) {
    if (colvis.length() == 0) {
      return EMPTY_VIS;
    }

    try {
      return visCache.get(colvis, new Callable<ColumnVisibility>() {
        @Override
        public ColumnVisibility call() throws Exception {
          return new ColumnVisibility(ByteUtil.toText(colvis));
        }
      });
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  public void validate(Column col) {
    getCV(col.getVisibility());
  }

  public void validate(Set<Column> columns) {
    for (Column column : columns) {
      validate(column);
    }

  }

}
