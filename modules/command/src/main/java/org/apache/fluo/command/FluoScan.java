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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.util.ScanUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FluoScan {

  public static class ScanOptions {

    @Parameter(names = "-a", required = true, description = "Fluo application name")
    private String applicationName;

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

    @Parameter(names = {"-D"}, description = "Sets configuration property. Expected format: <name>=<value>")
    private List<String> properties = new ArrayList<>();

    @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
    public boolean help;

    @Parameter(names = {"-esc", "--escape-non-ascii"}, help = true,
        description = "Hex encode non ascii bytes", arity = 1)
    public boolean hexEncNonAscii = true;

    @Parameter(
        names = "--raw",
        help = true,
        description = "Show underlying key/values stored in Accumulo. Interprets the data using Fluo "
            + "internal schema, making it easier to comprehend.")
    public boolean scanAccumuloTable = false;

    public String getApplicationName() {
      return applicationName;
    }

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

    List<String> getProperties() {
      return properties;
    }

    public ScanUtil.ScanOpts getScanOpts() {
      return new ScanUtil.ScanOpts(startRow, endRow, columns, exactRow, rowPrefix, help,
          hexEncNonAscii, scanAccumuloTable);
    }
  }

  public static void main(String[] args) {

    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger("org.apache.fluo").setLevel(Level.ERROR);

    ScanOptions options = new ScanOptions();
    JCommander jcommand = new JCommander(options);
    jcommand.setProgramName("fluo scan <app>");
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

    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.setApplicationName(options.getApplicationName());
    CommandUtil.overrideFluoConfig(config, options.getProperties());

    CommandUtil.verifyAppRunning(config);

    if (options.scanAccumuloTable) {
      ScanUtil.scanAccumulo(options.getScanOpts(), config);
    } else {
      ScanUtil.scanFluo(options.getScanOpts(), config);
    }
  }
}
