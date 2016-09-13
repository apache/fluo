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

import java.util.Objects;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.metrics.Counter;
import org.apache.fluo.api.metrics.Histogram;
import org.apache.fluo.api.metrics.Meter;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.metrics.Timer;
import org.apache.fluo.core.metrics.types.CounterImpl;
import org.apache.fluo.core.metrics.types.HistogramImpl;
import org.apache.fluo.core.metrics.types.MeterImpl;
import org.apache.fluo.core.metrics.types.TimerImpl;

/**
 * Implementation of {@link MetricsReporter} that reports application metrics using Fluo metrics
 */
public class MetricsReporterImpl implements MetricsReporter {

  private final FluoConfiguration config;
  private final MetricRegistry registry;
  private final String prefix;

  public MetricsReporterImpl(FluoConfiguration config, MetricRegistry registry,
      String metricsReporterID) {
    this.config = config;
    this.registry = registry;
    this.prefix = MetricNames.APPLICATION_PREFIX + "." + metricsReporterID + ".";
  }

  @Override
  public Counter counter(String metricName) {
    validateName(metricName);
    return new CounterImpl(registry.counter(prefix + metricName));
  }

  @Override
  public Histogram histogram(String metricName) {
    validateName(metricName);
    return new HistogramImpl(MetricsUtil.getHistogram(config, registry, prefix + metricName));
  }

  @Override
  public Meter meter(String metricName) {
    validateName(metricName);
    return new MeterImpl(registry.meter(prefix + metricName));
  }

  @Override
  public Timer timer(String metricName) {
    validateName(metricName);
    return new TimerImpl(MetricsUtil.getTimer(config, registry, prefix + metricName));
  }

  private static void validateName(String metricName) {
    Objects.requireNonNull(metricName);
    Preconditions.checkArgument(!metricName.contains("."), "Metric name " + metricName
        + " should not contain a period '.'");
  }
}
