/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.core.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TxLogger {
  private static final Logger log = LoggerFactory.getLogger(TxLogger.class);

  static void logTx(String status, String clazz, TxStats stats) {
    logTx(status, clazz, stats, null);
  }

  static void logTx(String status, String clazz, TxStats stats, String trigger) {
    if (log.isTraceEnabled()) {
      String triggerMsg = "";
      if (trigger != null)
        triggerMsg = "trigger:" + trigger + " ";

      // TODO need better names for #read and #ret... these indicate the number the user looked up and the number looked up that existed

      String msg = String.format("tx info thread:%d status:%s time:%d #read:TODO #ret:%,d #set:%,d #collisions:%,d waitTime:%,d %s class:%s", Thread
          .currentThread().getId(), status, stats.getTime(),
          stats.getEntriesReturned(), stats.getEntriesSet(), stats.getCollisions(), stats.getLockWaitTime(), triggerMsg, clazz);
      log.trace(msg);
    }
  }

  public static boolean isLoggingEnabled() {
    return log.isTraceEnabled();
  }
}
