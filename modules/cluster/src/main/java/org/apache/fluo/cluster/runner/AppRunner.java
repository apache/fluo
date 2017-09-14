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

package org.apache.fluo.cluster.runner;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Provider;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.cluster.util.FluoYarnConfig;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.ScanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for running a Fluo application
 */
@Deprecated
public abstract class AppRunner {

  private static final Logger log = LoggerFactory.getLogger(AppRunner.class);
  private static final long MIN_SLEEP_SEC = 10;
  private static final long MAX_SLEEP_SEC = 300;

  private String scriptName;

  public AppRunner(String scriptName) {
    this.scriptName = scriptName;
  }

  public void scan(FluoConfiguration config, String[] args) {
    ScanOptions options = new ScanOptions();
    JCommander jcommand = new JCommander(options);
    jcommand.setProgramName(scriptName + " scan <app>");
    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }

    if (options.help) {
      jcommand.usage();
      System.exit(0);
    }

    if (options.scanAccumuloTable) {
      ScanUtil.scanAccumulo(options.getScanOpts(), config);
    } else {
      ScanUtil.scanFluo(options.getScanOpts(), config);
    }
  }

  private long calculateSleep(long notifyCount, long numWorkers) {
    long sleep = notifyCount / numWorkers / 100;
    if (sleep < MIN_SLEEP_SEC) {
      return MIN_SLEEP_SEC;
    } else if (sleep > MAX_SLEEP_SEC) {
      return MAX_SLEEP_SEC;
    }
    return sleep;
  }

  @VisibleForTesting
  public long countNotifications(Environment env) {
    Scanner scanner = null;
    try {
      scanner = env.getConnector().createScanner(env.getTable(), env.getAuthorizations());
    } catch (TableNotFoundException e) {
      log.error("An exception was thrown -", e);
      throw new FluoException(e);
    }

    Notification.configureScanner(scanner);

    return Iterables.size(scanner);
  }

  public void waitUntilFinished(FluoConfiguration config) {
    try (Environment env = new Environment(config)) {
      log.info("The wait command will exit when all notifications are processed");
      while (true) {
        long ts1 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        long ntfyCount = countNotifications(env);
        long ts2 = env.getSharedResources().getOracleClient().getStamp().getTxTimestamp();
        if (ntfyCount == 0 && ts1 == (ts2 - 1)) {
          log.info("All processing has finished!");
          break;
        }

        try {
          long sleepSec = calculateSleep(ntfyCount, FluoYarnConfig.getWorkerInstances(config));
          log.info("{} notifications are still outstanding.  Will try again in {} seconds...",
              ntfyCount, sleepSec);
          Thread.sleep(1000 * sleepSec);
        } catch (InterruptedException e) {
          log.error("Sleep was interrupted!  Exiting...");
          System.exit(-1);
        }
      }
    } catch (FluoException e) {
      log.error(e.getMessage());
      System.exit(-1);
    } catch (Exception e) {
      log.error("An exception was thrown -", e);
      System.exit(-1);
    }
  }

  private static class FluoConfigModule extends AbstractModule {

    private Class<?> clazz;
    private FluoConfiguration fluoConfig;

    FluoConfigModule(Class<?> clazz, FluoConfiguration fluoConfig) {
      this.clazz = clazz;
      this.fluoConfig = fluoConfig;
    }

    @Override
    protected void configure() {
      requestStaticInjection(clazz);
      bind(FluoConfiguration.class).toProvider(new Provider<FluoConfiguration>() {
        @Override
        public FluoConfiguration get() {
          // TODO Auto-generated method stub
          return fluoConfig;
        }
      });
    }
  }

  public void exec(FluoConfiguration fluoConfig, String[] args) throws Exception {

    String className = args[0];
    Arrays.copyOfRange(args, 1, args.length);

    Class<?> clazz = Class.forName(className);

    // inject fluo configuration
    Guice.createInjector(new FluoConfigModule(clazz, fluoConfig));

    Method method = clazz.getMethod("main", String[].class);
    method.invoke(null, (Object) Arrays.copyOfRange(args, 1, args.length));
  }

  public static class ScanOptions {

    @Parameter(names = "-s", description = "Start row (inclusive) of scan")
    private String startRow;

    @Parameter(names = "-e", description = "End row (inclusive) of scan")
    private String endRow;

    @Parameter(names = "-c", description = "Columns of scan in comma separated format: "
        + "<<columnfamily>[:<columnqualifier>]{,<columnfamily>[:<columnqualifier>]}> ")
    private List<String> columns;

    @Parameter(names = "-r", description = "Exact row to scan")
    private String exactRow;

    @Parameter(names = "-p", description = "Row prefix to scan")
    private String rowPrefix;

    @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
    public boolean help;

    @Parameter(names = {"-esc", "--escape-non-ascii"}, help = true,
        description = "Hex encode non ascii bytes", arity = 1)
    public boolean hexEncNonAscii = true;

    @Parameter(names = "--raw", help = true,
        description = "Show underlying key/values stored in Accumulo. Interprets the data using Fluo "
            + "internal schema, making it easier to comprehend.")
    public boolean scanAccumuloTable = false;

    public String getStartRow() {
      return startRow;
    }

    public String getEndRow() {
      return endRow;
    }

    public String getExactRow() {
      return exactRow;
    }

    public String getRowPrefix() {
      return rowPrefix;
    }

    public List<String> getColumns() {
      if (columns == null) {
        return Collections.emptyList();
      }
      return columns;
    }

    public ScanUtil.ScanOpts getScanOpts() {
      return new ScanUtil.ScanOpts(startRow, endRow, columns, exactRow, rowPrefix, help,
          hexEncNonAscii, scanAccumuloTable);
    }
  }
}
