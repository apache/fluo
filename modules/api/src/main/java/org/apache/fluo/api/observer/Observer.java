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

package org.apache.fluo.api.observer;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.TransactionBase;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;

/**
 * Implemented by users to a watch a {@link Column} and be notified of changes to the Column via the
 * {@link #process(TransactionBase, Bytes, Column)} method. An observer is created for each worker
 * thread and reused for the lifetime of a worker thread. Consider extending
 * {@link AbstractObserver} as it will let you optionally implement {@link #init(Context)} and
 * {@link #close()}. The abstract class will also shield you from the addition of interface methods.
 *
 * @since 1.0.0
 */
public interface Observer {

  /**
   * @since 1.0.0
   */
  enum NotificationType {
    WEAK, STRONG
  }

  /**
   * A {@link Column} and {@link NotificationType} pair
   *
   * @since 1.0.0
   */
  class ObservedColumn {
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

  /**
   * @since 1.0.0
   */
  interface Context {
    /**
     * @return A configuration object with application configuration like that returned by
     *         {@link FluoClient#getAppConfiguration()}
     */
    SimpleConfiguration getAppConfiguration();

    /**
     * @return The per observer configuration that's specific to this observer.
     */
    SimpleConfiguration getObserverConfiguration();

  }

  /**
   * Implemented by user to initialize Observer.
   *
   * @param context Observer context
   */
  void init(Context context) throws Exception;

  /**
   * Implemented by users to process notifications on a {@link ObservedColumn}. If a notification
   * occurs, this method passes the user a {@link TransactionBase} as well as the row and
   * {@link Column} where the notification occurred. The user can use the {@link TransactionBase} to
   * read and write to Fluo. After this method returns, {@link TransactionBase} will be committed
   * and closed by Fluo.
   */
  void process(TransactionBase tx, Bytes row, Column col) throws Exception;

  /**
   * Implemented by user to return an {@link ObservedColumn} that will trigger this observer. During
   * initialization, this information is stored in zookeeper so that workers have a consistent view.
   * If a worker loads an Observer and the information returned differs from what is in zookeeper
   * then an exception will be thrown. It is safe to assume that {@link #init(Context)} will be
   * called before this method. If the return value of the method is derived from what is passed to
   * {@link #init(Context)}, then the derivation process should be deterministic.
   */
  ObservedColumn getObservedColumn();

  /**
   * Implemented by user to close resources used by Observer
   */
  void close();
}
