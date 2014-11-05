/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.metrics.config;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.codahale.metrics.MetricRegistry;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.metrics.ReporterStarter;
import org.apache.commons.configuration.Configuration;

public class ReporterStarterImpl implements ReporterStarter {

  @Override
  public List<AutoCloseable> start(Params params) {

    final FluoConfiguration conf = new FluoConfiguration(params.getConfiguration());

    ConfigurationSourceProvider csp = new ConfigurationSourceProvider() {
      @Override
      public InputStream open(String path) throws IOException {
        return conf.getMetricsYaml();
      }
    };

    try {
      return Reporters.startReporters(params.getMetricRegistry(), csp, "", params.getDomain());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) {
    new ReporterStarterImpl().start(new Params() {

      @Override
      public MetricRegistry getMetricRegistry() {
        return new MetricRegistry();
      }

      @Override
      public String getDomain() {

        return "test";
      }

      @Override
      public Configuration getConfiguration() {
        FluoConfiguration conf = new FluoConfiguration();
        conf.setMetricsYaml(new ByteArrayInputStream("---\nfrequency: 60 seconds\n".getBytes()));
        return conf;
      }
    });
  }
}
