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

package org.apache.fluo.core.util;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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

public class ScanUtil {

  public static Span getSpan(ScanOpts options) {
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

  public static Collection<Column> getColumns(ScanOpts options) {
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

  public static void scanFluo(ScanOpts options, FluoConfiguration sConfig) {
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

  public static void scanAccumulo(ScanOpts options, FluoConfiguration sConfig) {
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

  public static class ScanOpts {

    private String startRow;
    private String endRow;
    private List<String> columns;
    private String exactRow;
    private String rowPrefix;
    public boolean help;
    public boolean hexEncNonAscii = true;
    public boolean scanAccumuloTable = false;

    public ScanOpts(String startRow, String endRow, List<String> columns, String exactRow,
        String rowPrefix, boolean help, boolean hexEncNonAscii, boolean scanAccumuloTable) {
      this.startRow = startRow;
      this.endRow = endRow;
      this.columns = columns;
      this.exactRow = exactRow;
      this.rowPrefix = rowPrefix;
      this.help = help;
      this.hexEncNonAscii = hexEncNonAscii;
      this.scanAccumuloTable = scanAccumuloTable;
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
  }
}
