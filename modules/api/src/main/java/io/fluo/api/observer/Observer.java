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
package io.fluo.api.observer;

import java.util.Map;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.TransactionBase;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import org.apache.commons.configuration.Configuration;

/**
 * Implemented by users to a watch a {@link Column} and be notified of changes to the Column via the {@link #process(TransactionBase, Bytes, Column)} method. An
 * observer is created for each worker thread and reused for the lifetime of a worker thread. Consider extending {@link AbstractObserver} as it will let you
 * optionally implement {@link #init(Configuration)} and {@link #close()}. The abstract class will also shield you from the addition of interface methods.
 */
public interface Observer {

  public static enum NotificationType {
    WEAK, STRONG
  }

  /**
   * A {@link Column} and {@link NotificationType} pair
   */
  public static class ObservedColumn {
    private final Column col;
    private final NotificationType notificationType;

    public ObservedColumn(Column col, NotificationType notificationType) {
      this.col = col;
      this.notificationType = notificationType;
    }

    public Column getColumn() {
      return col;
    }

    public NotificationType getType() {
      return notificationType;
    }
  }

  public static interface Context {
    /**
     * @return A configuration object with application configuration like that returned by {@link FluoClient#getAppConfiguration()}
     */
    public Configuration getAppConfiguration();
    
    /**
     * @return The parameters configured for this observer
     */
    public Map<String, String> getParameters();
    
  }
  
  /**
   * Implemented by user to initialize Observer. 
   * 
   * @param config 
   */
  public void init(Context context) throws Exception;

  /**
   * Implemented by users to process notifications on a {@link ObservedColumn}. If a notification occurs, this method passes the user a {@link TransactionBase}
   * as well as the row and {@link Column} where the notification occurred. The user can use the {@TransactionBase} to read and write to Fluo.
   * After this method returns, {@link TransactionBase} will be committed and closed by Fluo.
   */
  public void process(TransactionBase tx, Bytes row, Column col) throws Exception;

  /**
   * Implemented by user to return an {@link ObservedColumn} that will trigger this observer. During initialization, this information is stored in zookeeper so
   * that workers have a consistent view. If a worker loads an Observer and the information returned differs from what is in zookeeper then an exception will be
   * thrown. It is safe to assume that {@link #init(Context)} will be called before this method. If the return value of the method is derived from what is passed to
   * {@link #init(Context)}, then the derivation process should be deterministic.
   */
  public ObservedColumn getObservedColumn();

  /**
   * Implemented by user to close resources used by Observer
   */
  public void close();
}
