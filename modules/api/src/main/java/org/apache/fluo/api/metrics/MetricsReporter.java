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

package org.apache.fluo.api.metrics;

/**
 * Reports application metrics using Fluo metrics reporters configured by 'fluo.metrics.reporter.*'
 * properties. Several types of metrics are supported which are described by Dropwizard docs
 * (http://metrics.dropwizard.io/3.1.0/getting-started/). Metrics should be identified by a unique
 * names to avoid conflicts. Periods "." should not be used in metric names.
 *
 * @since 1.0.0
 */
public interface MetricsReporter {

  /**
   * @return Metrics {@link Counter} identified by metricName
   */
  Counter counter(String metricName);

  /**
   * @return Metrics {@link Histogram} identified by metricName
   */
  Histogram histogram(String metricName);

  /**
   * @return Metrics {@link Meter} identified by metricName
   */
  Meter meter(String metricName);

  /**
   * @return Metrics {@link Timer} identified by metricName
   */
  Timer timer(String metricName);

}
