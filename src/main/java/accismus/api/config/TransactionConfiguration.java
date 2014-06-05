package accismus.api.config;

import java.util.concurrent.TimeUnit;

public interface TransactionConfiguration {
  public static final String ROLLBACK_TIME_PROP = "accismus.tx.rollbackTime";

  public void setRollbackTime(long time, TimeUnit tu);
}
