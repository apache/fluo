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

package org.apache.fluo.core.metrics.starters;

import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.core.metrics.ReporterStarter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleReporterStarter implements ReporterStarter {

  private static final Logger log = LoggerFactory.getLogger(ConsoleReporterStarter.class);

  @Override
  public List<AutoCloseable> start(Params params) {
    SimpleConfiguration config =
        new FluoConfiguration(params.getConfiguration()).getReporterConfiguration("console");

    if (!config.getBoolean("enable", false)) {
      return Collections.emptyList();
    }

    TimeUnit rateUnit = TimeUnit.valueOf(config.getString("rateUnit", "seconds").toUpperCase());
    TimeUnit durationUnit =
        TimeUnit.valueOf(config.getString("durationUnit", "milliseconds").toUpperCase());
    PrintStream out = System.out;
    if (config.getString("target", "stdout").equals("stderr")) {
      out = System.err;
    }

    ConsoleReporter reporter =
        ConsoleReporter.forRegistry(params.getMetricRegistry()).convertDurationsTo(durationUnit)
            .convertRatesTo(rateUnit).outputTo(out).build();
    reporter.start(config.getInt("frequency", 60), TimeUnit.SECONDS);

    log.info("Reporting metrics to console");

    return Collections.singletonList((AutoCloseable) reporter);
  }

}
