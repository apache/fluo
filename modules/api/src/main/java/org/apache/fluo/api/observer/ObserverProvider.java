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

import java.util.function.BiConsumer;

import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.observer.Observer.NotificationType;

/**
 * Fluo Workers use this class to register {@link Observer}s to process notifications.
 * Implementations of this class should register zero or more {@link Observer}s.
 *
 * <p>
 * When Fluo is initialized {@link #provideColumns(BiConsumer, Context)} is called. The columns it
 * registers are stored in Zookeeper. Transactions will use the columns stored in Zookeeper to
 * determine when to set notifications. When Workers call {@link #provide(Registry, Context)}, the
 * columns registered must be the same as those registered during initialization. If this is not the
 * case, then the worker will fail to start.
 *
 * @see FluoConfiguration#setObserverProvider(String)
 * @since 1.1.0
 */
public interface ObserverProvider {

  /**
   * @since 1.1.0
   */
  interface Context {
    /**
     * @return A configuration object with application configuration like that returned by
     *         {@link FluoClient#getAppConfiguration()}
     */
    SimpleConfiguration getAppConfiguration();

    /**
     * @return A {@link MetricsReporter} to report application metrics from observers.
     */
    MetricsReporter getMetricsReporter();
  }

  /**
   * Observers are registered with the worker using this interface. Registering an {@link Observer}s
   * relates it to the columns that trigger it.
   *
   * @since 1.1.0
   */
  interface Registry {
    void register(Column observedColumn, NotificationType ntfyType, Observer observer);

    /**
     * This method was created to allow Observers written as lambda to be passed {@link String}
     * instead of {@link Bytes} for the row.
     * 
     * <pre>
     * <code>
     *   void provide(ObserverRegistry or, Context ctx) {
     *     or.registers(someColumn, WEAK, (tx,row,col) -> {
     *      //row is of type String
     *     };
     *   }
     * </code>
     * </pre>
     */
    void registers(Column observedColumn, NotificationType ntfyType, StringObserver observer);
  }

  /**
   * This is method is called by Fluo Workers to register observers to process notifications.
   *
   * <p>
   * Observers registered may be called concurrently by multiple threads to process different
   * notifications. Observers should be tolerant of this.
   *
   * @param or Register observers with this.
   */
  void provide(Registry or, Context ctx);

  /**
   * Called during Fluo initialization to determine what columns are being observed. The default
   * implementation of this method calls {@link #provide(Registry, Context)} and ignores Observers.
   *
   * @param colRegistry register all observed columns with this consumer
   */
  default void provideColumns(BiConsumer<Column, NotificationType> colRegistry, Context ctx) {
    Registry or = new Registry() {
      @Override
      public void registers(Column oc, NotificationType nt, StringObserver obs) {
        colRegistry.accept(oc, nt);
      }

      @Override
      public void register(Column oc, NotificationType nt, Observer obs) {
        colRegistry.accept(oc, nt);
      }
    };

    provide(or, ctx);
  }
}
