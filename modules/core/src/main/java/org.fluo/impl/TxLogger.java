package org.fluo.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TxLogger {
  private static Logger log = LoggerFactory.getLogger(TxLogger.class);

  static void logTx(String status, String clazz, TxStats stats) {
    logTx(status, clazz, stats, null);
  }

  static void logTx(String status, String clazz, TxStats stats, String trigger) {
    if (log.isTraceEnabled()) {
      String triggerMsg = "";
      if (trigger != null)
        triggerMsg = "trigger:" + trigger + " ";

      // TODO need better names for #read and #ret... these indicate the number the user looked up and the number looked up that existed

      String msg = String.format("tx info thread:%d status:%s time:%d #read:TODO #ret:%,d #set:%,d #collisions:%,d waitTime:%,d %sclass:%s", Thread
          .currentThread().getId(), status, stats.getTime(),
          stats.getEntriesReturned(), stats.getEntriesSet(), stats.getCollisions(), stats.getLockWaitTime(), triggerMsg, clazz);
      log.trace(msg);
    }
  }

  public static boolean isLoggingEnabled() {
    return log.isTraceEnabled();
  }
}
