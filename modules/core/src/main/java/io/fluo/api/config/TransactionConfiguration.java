package io.fluo.api.config;

import java.util.concurrent.TimeUnit;

import static io.fluo.impl.Constants.FLUO_PREFIX;

public interface TransactionConfiguration {
  public static final String TRANSACTION_PREFIX = FLUO_PREFIX + ".tx";
  public static final String ROLLBACK_TIME_PROP = TRANSACTION_PREFIX + ".rollbackTime";

  public void setRollbackTime(long time, TimeUnit tu);
}
