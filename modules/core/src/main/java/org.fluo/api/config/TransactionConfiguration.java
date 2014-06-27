package org.fluo.api.config;

import java.util.concurrent.TimeUnit;

public interface TransactionConfiguration {
  public static final String ROLLBACK_TIME_PROP = "org.fluo.tx.rollbackTime";

  public void setRollbackTime(long time, TimeUnit tu);
}
