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
package io.fluo.cluster.runner;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.exceptions.FluoException;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;

/**
 * Base class for running a Fluo application
 */
public abstract class AppRunner {

  protected String fluoHomeDir;
  protected FluoConfiguration config;
  private String appName;
  private String scriptName;

  public AppRunner(String scriptName, String fluoHomeDir, FluoConfiguration config) {
    this.scriptName = scriptName;
    this.fluoHomeDir = fluoHomeDir;
    this.config = config;
    this.appName = config.getApplicationName();
  }

  public AppRunner(String scriptName, String fluoHomeDir, String appName) {
    this.scriptName = scriptName;
    this.fluoHomeDir = fluoHomeDir;
    this.appName = appName;
    File appPropsPath = new File(getAppPropsPath());
    if (!appPropsPath.exists()) {
      throw new IllegalStateException(appName+" application does not have a config path " + getAppPropsPath());
    }
    this.config = new FluoConfiguration(appPropsPath);
    if (!config.getApplicationName().equals(appName)) {
      throw new IllegalStateException("Application name in config '" + config.getApplicationName() + "' does not match given appName: " + appName);
    }
  }

  public String getConfDir() {
    return fluoHomeDir + "/conf";
  }

  public String getLibDir() {
    return fluoHomeDir + "/lib";
  }

  public String getAppsDir() {
    return fluoHomeDir + "/apps";
  }

  public String getAppPropsPath() {
    return getAppConfDir() + "/fluo.properties";
  }

  public String getAppConfDir() {
    return String.format("%s/%s/conf", getAppsDir(), appName);
  }

  public String getAppLibDir() {
    return String.format("%s/%s/lib", getAppsDir(), appName);
  }

  public FluoConfiguration getConfiguration() {
    return config;
  }



  public static ScannerConfiguration buildScanConfig(ScanOptions options) {
    ScannerConfiguration scanConfig = new ScannerConfiguration();

    if ((options.getExactRow() != null) && ((options.getStartRow() != null) || (options.getEndRow() != null) || (options.getRowPrefix() != null))) {
      throw new IllegalArgumentException("You cannot specify an exact row with a start/end row or row prefix!");
    }

    if ((options.getRowPrefix() != null) && ((options.getStartRow() != null) || (options.getEndRow() != null) || (options.getExactRow() != null))) {
      throw new IllegalArgumentException("You cannot specify an prefix row with a start/end row or exact row!");
    }

    // configure span of scanner
    if (options.getExactRow() != null) {
      scanConfig.setSpan(Span.exact(options.getExactRow()));
    } else if (options.getRowPrefix() != null) {
      scanConfig.setSpan(Span.prefix(options.getRowPrefix()));
    } else {
      if ((options.getStartRow() != null) && (options.getEndRow() != null)) {
        scanConfig.setSpan(new Span(options.getStartRow(), true, options.getEndRow(), true));
      } else if (options.getStartRow() != null) {
        scanConfig.setSpan(new Span(Bytes.of(options.getStartRow()), true, Bytes.EMPTY, true));
      } else if (options.getEndRow() != null) {
        scanConfig.setSpan(new Span(Bytes.EMPTY, true, Bytes.of(options.getEndRow()), true));
      }
    }

    // configure columns of scanner
    for (String column : options.getColumns()) {
      String[] colFields = column.split(":");
      if (colFields.length == 1) {
        scanConfig.fetchColumnFamily(Bytes.of(colFields[0]));
      } else if (colFields.length == 2) {
        scanConfig.fetchColumn(Bytes.of(colFields[0]), Bytes.of(colFields[1]));
      } else {
        throw new IllegalArgumentException("Failed to scan!  Column '" + column + "' has too many fields (indicated by ':')");
      }
    }

    return scanConfig;
  }

  public void scan(String[] args) {
    scan(config, args);
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

    // Limit client to retry for only 500ms as user is waiting
    FluoConfiguration sConfig = new FluoConfiguration(config);
    sConfig.setClientRetryTimeout(500);

    System.out.println("Scanning snapshot of data in Fluo '" + sConfig.getApplicationName() + "' application.");

    try (FluoClient client = FluoFactory.newClient(sConfig)) {
      try (Snapshot s = client.newSnapshot()) {

        ScannerConfiguration scanConfig = null;
        try {
          scanConfig = buildScanConfig(options);
        } catch (IllegalArgumentException e) {
          System.err.println(e.getMessage());
          System.exit(-1);
        }

        RowIterator iter = s.get(scanConfig);

        if (iter.hasNext() == false) {
          System.out.println("\nNo data found\n");
        }

        while (iter.hasNext() && !System.out.checkError()) {
          Map.Entry<Bytes,ColumnIterator> rowEntry = iter.next();
          ColumnIterator citer = rowEntry.getValue();
          while (citer.hasNext() && !System.out.checkError()) {
            Map.Entry<Column,Bytes> colEntry = citer.next();
            System.out.println(rowEntry.getKey() + " " + colEntry.getKey() + "\t" + colEntry.getValue());
          }
        }
      } catch (FluoException e) {
        System.out.println("Scan failed - " + e.getMessage());
      }
    }
  }
}
