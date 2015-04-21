/*
 * Copyright 2014 Fluo authors (see AUTHORS)
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
package io.fluo.metrics.config;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.validation.Validation;
import javax.validation.Validator;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.dropwizard.configuration.ConfigurationFactory;
import io.dropwizard.configuration.ConfigurationSourceProvider;
import io.dropwizard.configuration.FileConfigurationSourceProvider;
import io.dropwizard.jackson.Jackson;
import io.dropwizard.metrics.MetricsFactory;
import io.dropwizard.metrics.ReporterFactory;
import io.fluo.api.config.FluoConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reporters implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(Reporters.class);

  private final List<AutoCloseable> reporters;

  private Reporters(List<AutoCloseable> reporters) {
    this.reporters = reporters;
  }

  @Override
  public void close() {
    for (AutoCloseable reporter : reporters) {
      try {
        reporter.close();
      } catch (Exception e) {
        log.warn("Failed to stop " + reporter.getClass().getName(), e);
      }
    }
  }

  static List<AutoCloseable> startReporters(MetricRegistry registry, File yaml) throws Exception {
    ConfigurationSourceProvider csp = null;
    if (yaml.exists()) {
      csp = new FileConfigurationSourceProvider();
    }

    return startReporters(registry, csp, yaml.toString(), null);
  }

  static List<AutoCloseable> startReporters(MetricRegistry registry,
      ConfigurationSourceProvider csp, String path, String domain) throws Exception {

    // this method was intentionally put in metrics module instead of core module inorder to avoid
    // pulling in extra deps for core.

    List<AutoCloseable> reporterList = new ArrayList<>();

    if (csp != null) {
      ObjectMapper objectMapper = Jackson.newObjectMapper();
      Validator validator = Validation.buildDefaultValidatorFactory().getValidator();
      ConfigurationFactory<MetricsFactory> configFactory =
          new ConfigurationFactory<>(MetricsFactory.class, validator, objectMapper, "dw");

      MetricsFactory metricsFactory = configFactory.build(csp, path);
      ImmutableList<ReporterFactory> reporters = metricsFactory.getReporters();

      for (ReporterFactory reporterFactory : reporters) {
        ScheduledReporter reporter = reporterFactory.build(registry);
        reporter.start(metricsFactory.getFrequency().toMilliseconds(), TimeUnit.MILLISECONDS);
        log.debug("Started reporter " + reporter.getClass().getName());
        reporterList.add(reporter);
      }
    }

    // see dropwizard PR #552 (https://github.com/dropwizard/dropwizard/pull/552)
    if (domain == null)
      domain = FluoConfiguration.FLUO_PREFIX;
    JmxReporter jmxReporter = JmxReporter.forRegistry(registry).inDomain(domain).build();
    jmxReporter.start();
    log.debug("Started reporter " + jmxReporter.getClass().getName());
    reporterList.add(jmxReporter);

    return reporterList;
  }

  public static Reporters init(String configDir, MetricRegistry registry) {
    File metricsFile = new File(configDir, "metrics.yaml");
    try {
      return new Reporters(startReporters(registry, metricsFile));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
