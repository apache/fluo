package io.fluo.impl;

import io.fluo.api.Bytes;
import io.fluo.api.Column;

import java.util.concurrent.TimeUnit;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;

public class TxInfoCache {

  final static int CACHE_TIMEOUT_MIN = 24 * 60;

  private static class TxStatusWeigher implements Weigher<PrimaryRowColumn,TxInfo> {
    @Override
    public int weigh(PrimaryRowColumn key, TxInfo value) {
      return key.weight() + (value.lockValue == null ? 0 : value.lockValue.length) + 24;
    }
  }

  private Cache<PrimaryRowColumn,TxInfo> cache;
  private Configuration config;

  TxInfoCache(Configuration config) {
    cache = CacheBuilder.newBuilder().expireAfterAccess(CACHE_TIMEOUT_MIN, TimeUnit.MINUTES).maximumWeight(10000000).weigher(new TxStatusWeigher())
        .concurrencyLevel(10).build();
    this.config = config;
  }

  public TxInfo getTransactionInfo(final Bytes prow, final Column pcol, final long startTs) {
    return getTransactionInfo(new PrimaryRowColumn(prow, pcol, startTs));
  }

  public TxInfo getTransactionInfo(PrimaryRowColumn key) {

    TxInfo txInfo = cache.getIfPresent(key);
    if (txInfo == null) {
      txInfo = TxInfo.getTransactionInfo(config, key.prow, key.pcol, key.startTs);
      if (txInfo.status == TxStatus.ROLLED_BACK || txInfo.status == TxStatus.COMMITTED) {
        // only cache for these statuses which are not expected to change, other status can change over time
        cache.put(key, txInfo);
      }
    }

    return txInfo;
  }
}
