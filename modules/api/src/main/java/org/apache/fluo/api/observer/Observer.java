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
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.metrics.MetricsReporter;

/**
 * Implemented by users to a watch a {@link Column} and be notified of changes to the Column via the
 * {@link #process(TransactionBase, Bytes, Column)} method.
 * 
 * <p>
 * In Fluo version 1.1.0 this was converted to a functional interface. This change along with the
 * introduction of {@link ObserverProvider} allows Observers to be written as lambdas.
 *
 * @since 1.0.0
 */
@FunctionalInterface
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
   * @deprecated since 1.1.0. The method that used this class was deprecated.
   */
  @Deprecated
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

    /**
     * @since 1.1.0
     */
    @Override
    public String toString() {
      return col + " " + notificationType;
    }
  }

  /**
   * @since 1.0.0
   *
   * @deprecated since 1.1.0. The method that used this interface was deprecated.
   */
  @Deprecated
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

    /**
     * @return A {@link MetricsReporter} to report application metrics from this observer
     */
    MetricsReporter getMetricsReporter();
  }

  /**
   * Implemented by user to initialize Observer.
   *
   * @param context Observer context
   *
   * @deprecated since 1.1.0. Fluo will no longer call this method when observers are configured by
   *             {@link FluoConfiguration#setObserverProvider(String)}. Its only called when
   *             observers are configured the old way by
   *             {@link FluoConfiguration#addObserver(org.apache.fluo.api.config.ObserverSpecification)}
   */
  @Deprecated
  default void init(Context context) throws Exception {}

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
   *
   * @deprecated since 1.1.0 Fluo will no longer call this method when observers are configured by
   *             {@link FluoConfiguration#setObserverProvider(String)}. Its only called when
   *             observers are configured the old way by
   *             {@link FluoConfiguration#addObserver(org.apache.fluo.api.config.ObserverSpecification)}
   */
  @Deprecated
  default ObservedColumn getObservedColumn() {
    throw new UnsupportedOperationException();
  }

  /**
   * Implemented by user to close resources used by Observer
   *
   * @deprecated since 1.1.0. Fluo will no longer call this method when observers are configured by
   *             {@link FluoConfiguration#setObserverProvider(String)}. Its only called when
   *             observers are configured the old way by
   *             {@link FluoConfiguration#addObserver(org.apache.fluo.api.config.ObserverSpecification)}
   */
  @Deprecated
  default void close() {}
}
