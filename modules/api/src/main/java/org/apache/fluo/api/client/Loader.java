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

package org.apache.fluo.api.client;

import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.metrics.MetricsReporter;

/**
 * Interface that is implemented by users to load data into Fluo. Loader classes are executed by a
 * {@link LoaderExecutor}.
 *
 * @since 1.0.0
 */
@FunctionalInterface
public interface Loader {

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
     * @return A {@link MetricsReporter} to report application metrics from this loader
     */
    MetricsReporter getMetricsReporter();
  }

  /**
   * Users implement this method to load data into Fluo using the provided {@link TransactionBase}.
   * The transaction will be committed and closed by Fluo after this method returns
   */
  void load(TransactionBase tx, Context context) throws Exception;
}
