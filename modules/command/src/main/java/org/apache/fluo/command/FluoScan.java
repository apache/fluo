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

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
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
import org.apache.fluo.core.util.AccumuloUtil;
import org.apache.fluo.core.util.ByteUtil;
import org.apache.fluo.core.util.Hex;
import org.apache.fluo.core.util.SpanUtil;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class FluoScan {

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

    @Parameter(
        names = "--raw",
        help = true,
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

  private static void scanFluo(ScanOptions options, FluoConfiguration sConfig) {
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
  }

  private static void scanAccumulo(ScanOptions options, FluoConfiguration sConfig) {
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
    }
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.err.println("Usage: FluoScan <connectionPropsPath> <appName> userArgs...");
      System.exit(-1);
    }
    final String connectionPropsPath = args[0];
    final String applicationName = args[1];
    Objects.requireNonNull(connectionPropsPath);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    String[] userArgs = Arrays.copyOfRange(args, 2, args.length);

    Logger.getRootLogger().setLevel(Level.ERROR);
    Logger.getLogger("org.apache.fluo").setLevel(Level.ERROR);

    ScanOptions options = new ScanOptions();
    JCommander jcommand = new JCommander(options);
    jcommand.setProgramName("fluo scan <app>");
    try {
      jcommand.parse(userArgs);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }

    if (options.help) {
      jcommand.usage();
      System.exit(0);
    }

    FluoConfiguration config = new FluoConfiguration(connectionPropsFile);
    config.setApplicationName(applicationName);
    CommandUtil.verifyAppRunning(config);

    if (options.scanAccumuloTable) {
      scanAccumulo(options, config);
    } else {
      scanFluo(options, config);
    }
  }
}
