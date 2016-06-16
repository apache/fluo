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

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Timer;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.impl.FluoConfigurationImpl;
import org.mpierce.metrics.reservoir.hdrhistogram.HdrHistogramResetOnSnapshotReservoir;

public class MetricsUtil {

  public static Reservoir getConfiguredReservoir(FluoConfiguration config) {
    String clazz =
        config.getString(FluoConfigurationImpl.METRICS_RESERVOIR_PROP,
            HdrHistogramResetOnSnapshotReservoir.class.getName());
    try {
      return Class.forName(clazz).asSubclass(Reservoir.class).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  public static synchronized Timer addTimer(FluoConfiguration config, MetricRegistry registry,
      String name) {
    Timer timer = registry.getTimers().get(name);
    if (timer == null) {
      timer = new Timer(getConfiguredReservoir(config));
      registry.register(name, timer);
    }
    return timer;
  }

  public static Timer getTimer(FluoConfiguration config, MetricRegistry registry, String name) {
    Timer timer = registry.getTimers().get(name);
    if (timer == null) {
      return addTimer(config, registry, name);
    }
    return timer;
  }

  public static synchronized Histogram addHistogram(FluoConfiguration config,
      MetricRegistry registry, String name) {
    Histogram histogram = registry.getHistograms().get(name);
    if (histogram == null) {
      histogram = new Histogram(getConfiguredReservoir(config));
      registry.register(name, histogram);
    }
    return histogram;
  }

  public static Histogram getHistogram(FluoConfiguration config, MetricRegistry registry,
      String name) {
    Histogram histogram = registry.getHistograms().get(name);
    if (histogram == null) {
      return addHistogram(config, registry, name);
    }
    return histogram;
  }
}
