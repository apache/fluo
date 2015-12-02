/*
 * Copyright 2015 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.metrics.starters;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.metrics.ReporterStarter;
import org.apache.commons.configuration.Configuration;

public class GraphiteReporterStarter implements ReporterStarter {

  @Override
  public List<AutoCloseable> start(Params params) {
    Configuration config =
        new FluoConfiguration(params.getConfiguration()).getReporterConfiguration("graphite");

    if (!config.getBoolean("enable", false)) {
      return Collections.emptyList();
    }

    String host = config.getString("host");
    String prefix = config.getString("prefix", "");
    int port = config.getInt("port", 8080);
    TimeUnit rateUnit = TimeUnit.valueOf(config.getString("rateUnit", "seconds").toUpperCase());
    TimeUnit durationUnit =
        TimeUnit.valueOf(config.getString("durationUnit", "milliseconds").toUpperCase());

    Graphite graphite = new Graphite(host, port);
    GraphiteReporter reporter =
        GraphiteReporter.forRegistry(params.getMetricRegistry()).convertDurationsTo(durationUnit)
            .convertRatesTo(rateUnit).prefixedWith(prefix).build(graphite);
    reporter.start(config.getInt("frequency", 60), TimeUnit.SECONDS);

    return Collections.singletonList((AutoCloseable) reporter);
  }
}
