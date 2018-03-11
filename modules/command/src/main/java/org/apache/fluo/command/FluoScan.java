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

package org.apache.fluo.command;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;
import org.apache.fluo.core.util.ScanUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FluoScan {

  public static class ScanOptions extends CommonOpts {

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

    @Parameter(names = {"-esc", "--escape-non-ascii"}, help = true,
        description = "Hex encode non ascii bytes", arity = 1)
    public boolean hexEncNonAscii = true;

    @Parameter(names = "--raw", help = true,
        description = "Show underlying key/values stored in Accumulo. Interprets the data using Fluo "
            + "internal schema, making it easier to comprehend.")
    public boolean scanAccumuloTable = false;

    @Parameter(names = "--csv", help = true,
        description = "Export key/values stored in Accumulo as CSV file. Uses Fluo application "
            + "properties to configure the CSV format.")
    public boolean exportAsCsv = false;

    @Parameter(names = "--csv-header", help = true, description = "Set header for \"true\".")
    public String csvHeader;

    @Parameter(names = "--csv-delimiter", help = true,
        description = "Configure delimiter to a designeted character.")
    public String csvDelimiter;

    @Parameter(names = "--csv-escape", help = true,
        description = "Configure escape to a designeted character.")
    public String csvEscape;

    @Parameter(names = "--csv-quote", help = true,
        description = "Configure quote to a designeted character.")
    public String csvQuote;

    @Parameter(names = "--csv-quote-mode", help = true,
        description = "Configure quote mode to a designeted mode. The possible "
                + "modes are: ALL, ALL_NON_NULL, MINIMAL, NONE and NON_NUMERIC")
    public String csvQuoteMode;

    @Parameter(names = "--json", help = true,
        description = "Export key/values stored in Accumulo as JSON file.")
    public boolean exportAsJson = false;

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

    public String getCsvHeader() {
      return csvHeader;
    }

    public String getCsvDelimiter() {
      return csvDelimiter;
    }

    public String getCsvEscape() {
      return csvEscape;
    }

    public String getCsvQuote() {
      return csvQuote;
    }

    public String getCsvQuoteMode() {
      return csvQuoteMode;
    }

    public List<String> getColumns() {
      if (columns == null) {
        return Collections.emptyList();
      }
      return columns;
    }

    /**
     * Check if the parameters informed can be used together.
     * @since 1.2
     */
    private void checkScanOptions() {
      if (this.exportAsCsv && this.exportAsJson) {
        throw new IllegalArgumentException(
            "Both \"--csv\" and \"--json\" can not be set together.");
      }

      if (!this.exportAsCsv && (StringUtils.isNotEmpty(this.csvDelimiter)
          | StringUtils.isNotEmpty(this.csvEscape) | StringUtils.isNotEmpty(this.csvHeader)
          | StringUtils.isNotEmpty(this.csvQuote) | StringUtils.isNotEmpty(this.csvQuoteMode))) {
        throw new IllegalArgumentException("No \"--csv\" detected");
      }
    }

    public ScanUtil.ScanOpts getScanOpts() {
      return new ScanUtil.ScanOpts(startRow, endRow, columns, exactRow, rowPrefix, help,
          hexEncNonAscii, scanAccumuloTable, exportAsCsv, csvDelimiter, csvEscape, csvHeader,
          csvQuote, csvQuoteMode, exportAsJson);
    }

    public static ScanOptions parse(String[] args) {
      ScanOptions opts = new ScanOptions();
      parse("fluo scan", opts, args);
      return opts;
    }
  }

  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger("org.apache.fluo").setLevel(Level.ERROR);

    ScanOptions options = ScanOptions.parse(args);
    options.checkScanOptions();
    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.setApplicationName(options.getApplicationName());
    options.overrideFluoConfig(config);
    CommandUtil.verifyAppRunning(config);

    try {
      options.overrideFluoConfig(config);
      if (options.scanAccumuloTable) {
        config = FluoAdminImpl.mergeZookeeperConfig(config);
        ScanUtil.scanAccumulo(options.getScanOpts(), config, System.out);
      } else {
        ScanUtil.scanFluo(options.getScanOpts(), config, System.out);
      }
    } catch (RuntimeException | IOException e) {
      System.err.println("Scan failed - " + e.getMessage());
      System.exit(-1);
    }
  }

}
