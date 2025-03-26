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

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

public class TxInfoCache {

  private static class TxStatusWeigher implements Weigher<PrimaryRowColumn, TxInfo> {
    @Override
    public int weigh(PrimaryRowColumn key, TxInfo value) {
      return key.weight() + (value.getLockValue() == null ? 0 : value.getLockValue().length) + 24;
    }
  }

  private final Cache<PrimaryRowColumn, TxInfo> cache;
  private final Environment env;

  TxInfoCache(Environment env) {
    final FluoConfiguration conf = env.getConfiguration();
    cache = CacheBuilder.newBuilder()
        .expireAfterAccess(FluoConfigurationImpl.getTxIfoCacheTimeout(conf, TimeUnit.MILLISECONDS),
            TimeUnit.MILLISECONDS)
        .maximumWeight(FluoConfigurationImpl.getTxInfoCacheWeight(conf))
        .weigher(new TxStatusWeigher()).concurrencyLevel(10).build();
    this.env = env;
  }

  public TxInfo getTransactionInfo(final Bytes prow, final Column pcol, final long startTs) {
    return getTransactionInfo(new PrimaryRowColumn(prow, pcol, startTs));
  }

  public TxInfo getTransactionInfo(PrimaryRowColumn key) {

    TxInfo txInfo = cache.getIfPresent(key);
    if (txInfo == null) {
      txInfo = TxInfo.getTransactionInfo(env, key.prow, key.pcol, key.startTs);
      if (txInfo.getStatus() == TxStatus.ROLLED_BACK || txInfo.getStatus() == TxStatus.COMMITTED) {
        // only cache for these statuses which are not expected to change, other status can change
        // over time
        cache.put(key, txInfo);
      }
    }
    return txInfo;
  }
}
