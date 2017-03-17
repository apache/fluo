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
 * Fluo Workers use this class to create {@link Observer}s to process notifications. Implementations
 * of this class should emit zero or more {@link Observer}s.
 *
 * <p>
 * When Fluo is initialized {@link #getObservedColumns(Context, BiConsumer)} is called. The columns
 * it emits are stored in Zookeeper. Transactions will use the columns stored in Zookeeper to
 * determine when to set notifications. When Workers call
 * {@link #createObservers(ObserverConsumer, Context)}, the columns emitted must be the same as
 * those emitted during initialization. If this is not the case, then the worker will fail to start.
 *
 * @see FluoConfiguration#setObserversFactory(String)
 * @since 1.1.0
 */
public interface ObserversFactory {

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
   * Observers are emitted to the worker using this interface. This interface also allows
   * {@link Observer}s to be related to the columns that trigger them.
   *
   * @since 1.1.0
   */
  interface ObserverConsumer {
    void accept(Column observedColumn, NotificationType ntfyType, Observer observer);

    /**
     * This method was created to allow Observers written as lambda to be passed {@link String}
     * instead of {@link Bytes}.
     * 
     * <pre>
     * <code>
     *   void createObservers(ObserverConsumer obsConsumer, Context ctx) {
     *     obsConsumer.accepts(someColumn, WEAK, (tx,row,col) -> {
     *      //row is of type String
     *     };
     *   }
     * </code>
     * </pre>
     */
    void accepts(Column observedColumn, NotificationType ntfyType, StringObserver observer);
  }

  /**
   * This is method is called by Fluo Workers to create observers to process notifications.
   *
   * <p>
   * Observers emitted may be called concurrently by multiple threads to process different
   * notifications. Observers should be tolerant of this.
   *
   * @param obsConsumer Emit an applications observers to this consumer.
   */
  void createObservers(ObserverConsumer obsConsumer, Context ctx);

  /**
   * Called during Fluo initialization to determine what columns are being observed. The default
   * implementation of this method calls {@link #createObservers(ObserverConsumer, Context)} and
   * ignores the Observers.
   *
   * @param obsColConsumer pass all observed columns to this consumer
   */
  default void getObservedColumns(Context ctx, BiConsumer<Column, NotificationType> obsColConsumer) {
    ObserverConsumer obsConsumer = new ObserverConsumer() {
      @Override
      public void accepts(Column oc, NotificationType nt, StringObserver obs) {
        obsColConsumer.accept(oc, nt);
      }

      @Override
      public void accept(Column oc, NotificationType nt, Observer obs) {
        obsColConsumer.accept(oc, nt);
      }
    };

    createObservers(obsConsumer, ctx);
  }
}
