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

package org.apache.fluo.core.metrics;

import java.util.List;

import com.codahale.metrics.MetricRegistry;
import org.apache.fluo.api.config.SimpleConfiguration;

public interface ReporterStarter {

  interface Params {
    /**
     *
     * @return Fluo's configuration
     */
    SimpleConfiguration getConfiguration();

    /**
     *
     * @return The metric registry used by Fluo
     */
    MetricRegistry getMetricRegistry();

    String getDomain();
  }

  /**
   * A fluo extension point that allows configuration of metrics reporters. This method should
   * configure and start reporters.
   *
   * @param params Elements of Fluo environment needed to setup reporters.
   * @return A list of closeables which represent reporters.
   */
  List<AutoCloseable> start(Params params);

}
