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
import java.util.Collection;
import java.util.HashSet;

import javax.inject.Provider;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.accumulo.format.FluoFormatter;
import org.apache.fluo.api.client.FluoClient;
import org.apache.fluo.api.client.FluoFactory;
import org.apache.fluo.api.client.Snapshot;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.cluster.util.FluoYarnConfig;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.Notification;
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.Hex;
import org.apache.fluo.core.util.SpanUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for running a Fluo application
 */
public abstract class AppRunner {

  private static final Logger log = LoggerFactory.getLogger(AppRunner.class);
  private static final long MIN_SLEEP_SEC = 10;
  private static final long MAX_SLEEP_SEC = 300;

  private String scriptName;

  public AppRunner(String scriptName) {
    this.scriptName = scriptName;
  }

  public static Span getSpan(ScanOptions options) {
    Span span = new Span();
    if ((options.getExactRow() != null)
        && ((options.getStartRow() != null) || (options.getEndRow() != null) || (options
            .getRowPrefix() != null))) {
      throw new IllegalArgumentException(
          "You cannot specify an exact row with a start/end row or row prefix!");
    }

    if ((options.getRowPrefix() != null)
        && ((options.getStartRow() != null) || (options.getEndRow() != null) || (options
            .getExactRow() != null))) {
      throw new IllegalArgumentException(
          "You cannot specify an prefix row with a start/end row or exact row!");
    }

    // configure span of scanner
    if (options.getExactRow() != null) {
      span = Span.exact(options.getExactRow());
    } else if (options.getRowPrefix() != null) {
      span = Span.prefix(options.getRowPrefix());
    } else {
      if ((options.getStartRow() != null) && (options.getEndRow() != null)) {
        span = new Span(options.getStartRow(), true, options.getEndRow(), true);
      } else if (options.getStartRow() != null) {
        span = new Span(Bytes.of(options.getStartRow()), true, Bytes.EMPTY, true);
      } else if (options.getEndRow() != null) {
        span = new Span(Bytes.EMPTY, true, Bytes.of(options.getEndRow()), true);
      }
    }

    return span;
  }

  public static Collection<Column> getColumns(ScanOptions options) {
    Collection<Column> columns = new HashSet<>();

    // configure columns of scanner
    for (String column : options.getColumns()) {
      String[] colFields = column.split(":");
      if (colFields.length == 1) {
        columns.add(new Column(colFields[0]));
      } else if (colFields.length == 2) {
        columns.add(new Column(colFields[0], colFields[1]));
      } else {
        throw new IllegalArgumentException("Failed to scan!  Column '" + column
            + "' has too many fields (indicated by ':')");
      }
    }

    return columns;
  }



  public long scan(FluoConfiguration config, String[] args) {
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
      return scanAccumulo(options, config);
    } else {
      return scanFluo(options, config);
    }
  }

  private long scanFluo(ScanOptions options, FluoConfiguration sConfig) {
    System.out.println("Scanning snapshot of data in Fluo '" + sConfig.getApplicationName()
        + "' application.");

    long entriesFound = 0;
    try (FluoClient client = FluoFactory.newClient(sConfig)) {
      try (Snapshot s = client.newSnapshot()) {

        Span span = null;
        Collection<Column> columns = null;
        try {
          span = getSpan(options);
          columns = getColumns(options);
        } catch (IllegalArgumentException e) {
          System.err.println(e.getMessage());
          System.exit(-1);
        }

        CellScanner cellScanner = s.scanner().over(span).fetch(columns).build();

        StringBuilder sb = new StringBuilder();
        for (RowColumnValue rcv : cellScanner) {
          if (options.hexEncNonAscii) {
            sb.setLength(0);
            Hex.encNonAscii(sb, rcv.getRow());
            sb.append(" ");
            Hex.encNonAscii(sb, rcv.getColumn(), " ");
            sb.append("\t");
            Hex.encNonAscii(sb, rcv.getValue());
            System.out.println(sb.toString());
          } else {
            sb.setLength(0);
            sb.append(rcv.getsRow());
            sb.append(" ");
            sb.append(rcv.getColumn());
            sb.append("\t");
            sb.append(rcv.getsValue());
            System.out.println(sb.toString());
          }
          entriesFound++;
          if (System.out.checkError()) {
            break;
          }
        }

        if (entriesFound == 0) {
          System.out.println("\nNo data found\n");
        }
      } catch (FluoException e) {
        System.out.println("Scan failed - " + e.getMessage());
      }
    }
    return entriesFound;
  }

  private long scanAccumulo(ScanOptions options, FluoConfiguration sConfig) {
    System.out.println("Scanning data in Accumulo directly for '" + sConfig.getApplicationName()
        + "' application.");

    Connector conn = AccumuloUtil.getConnector(sConfig);

    Span span = null;
    Collection<Column> columns = null;
    try {
      span = getSpan(options);
      columns = getColumns(options);
    } catch (IllegalArgumentException e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    }

    long entriesFound = 0;

    try {
      Scanner scanner = conn.createScanner(sConfig.getAccumuloTable(), Authorizations.EMPTY);
      scanner.setRange(SpanUtil.toRange(span));
      for (Column col : columns) {
        if (col.isQualifierSet()) {
          scanner
              .fetchColumn(ByteUtil.toText(col.getFamily()), ByteUtil.toText(col.getQualifier()));
        } else {
          scanner.fetchColumnFamily(ByteUtil.toText(col.getFamily()));
        }
      }

      for (String entry : Iterables.transform(scanner, FluoFormatter::toString)) {
        System.out.println(entry);
      }
    } catch (Exception e) {
      System.out.println("Scan failed - " + e.getMessage());
      entriesFound++;
    }

    return entriesFound;
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
}
