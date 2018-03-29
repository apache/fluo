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

import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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

import com.google.common.collect.Iterables;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;

public class ScanUtil {
  public static final String FLUO_VALUE = "value";
  public static final String FLUO_COLUMN_VISIBILITY = "visibility";
  public static final String FLUO_COLUMN_QUALIFIER = "qualifier";
  public static final String FLUO_COLUMN_FAMILY = "family";
  public static final String FLUO_ROW = "row";

  public static Span getSpan(ScanOpts options) {
    Span span = new Span();
    if ((options.getExactRow() != null) && ((options.getStartRow() != null)
        || (options.getEndRow() != null) || (options.getRowPrefix() != null))) {
      throw new IllegalArgumentException(
          "You cannot specify an exact row with a start/end row or row prefix!");
    }

    if ((options.getRowPrefix() != null) && ((options.getStartRow() != null)
        || (options.getEndRow() != null) || (options.getExactRow() != null))) {
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
        throw new IllegalArgumentException(
            "Failed to scan!  Column '" + column + "' has too many fields (indicated by ':')");
      }
    }

    return columns;
  }


  private static Function<Bytes, String> getEncoder(ScanOpts options) {
    if (options.hexEncNonAscii) {
      return Hex::encNonAscii;
    } else {
      return Bytes::toString;
    }
  }

  public static void scanFluo(ScanOpts options, FluoConfiguration sConfig, PrintStream out)
      throws IOException {

    try (FluoClient client = FluoFactory.newClient(sConfig)) {
      try (Snapshot s = client.newSnapshot()) {

        Span span = getSpan(options);
        Collection<Column> columns = getColumns(options);
        CellScanner cellScanner = s.scanner().over(span).fetch(columns).build();
        Function<Bytes, String> encoder = getEncoder(options);

        if (options.exportAsJson) {
          generateJson(cellScanner, encoder, out);
        } else {
          for (RowColumnValue rcv : cellScanner) {
            out.print(encoder.apply(rcv.getRow()));
            out.print(' ');
            out.print(encoder.apply(rcv.getColumn().getFamily()));
            out.print(' ');
            out.print(encoder.apply(rcv.getColumn().getQualifier()));
            out.print(' ');
            out.print(encoder.apply(rcv.getColumn().getVisibility()));
            out.print("\t");
            out.print(encoder.apply(rcv.getValue()));
            out.println();
            if (out.checkError()) {
              break;
            }
          }
        }
      }
    }
  }

  /**
   * Generate JSON format as result of the scan.
   *
   * @since 1.2
   */
  private static void generateJson(CellScanner cellScanner, Function<Bytes, String> encoder,
      PrintStream out) throws JsonIOException {
    Gson gson = new GsonBuilder().serializeNulls().setDateFormat(DateFormat.LONG)
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setVersion(1.0)
        .create();

    Map<String, String> json = new LinkedHashMap<>();
    for (RowColumnValue rcv : cellScanner) {
      json.put(FLUO_ROW, encoder.apply(rcv.getRow()));
      json.put(FLUO_COLUMN_FAMILY, encoder.apply(rcv.getColumn().getFamily()));
      json.put(FLUO_COLUMN_QUALIFIER, encoder.apply(rcv.getColumn().getQualifier()));
      json.put(FLUO_COLUMN_VISIBILITY, encoder.apply(rcv.getColumn().getVisibility()));
      json.put(FLUO_VALUE, encoder.apply(rcv.getValue()));
      gson.toJson(json, out);
      out.append("\n");

      if (out.checkError()) {
        break;
      }
    }
    out.flush();
  }

  public static void scanAccumulo(ScanOpts options, FluoConfiguration sConfig, PrintStream out) {

    Connector conn = AccumuloUtil.getConnector(sConfig);

    Span span = getSpan(options);
    Collection<Column> columns = getColumns(options);

    try {
      Scanner scanner = conn.createScanner(sConfig.getAccumuloTable(), Authorizations.EMPTY);
      scanner.setRange(SpanUtil.toRange(span));
      for (Column col : columns) {
        if (col.isQualifierSet()) {
          scanner.fetchColumn(ByteUtil.toText(col.getFamily()),
              ByteUtil.toText(col.getQualifier()));
        } else {
          scanner.fetchColumnFamily(ByteUtil.toText(col.getFamily()));
        }
      }

      for (String entry : Iterables.transform(scanner, FluoFormatter::toString)) {
        out.println(entry);
      }
      out.flush();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
    public boolean exportAsJson = false;

    public ScanOpts(String startRow, String endRow, List<String> columns, String exactRow,
        String rowPrefix, boolean help, boolean hexEncNonAscii, boolean scanAccumuloTable,
        boolean exportAsJson) {
      this.startRow = startRow;
      this.endRow = endRow;
      this.columns = columns;
      this.exactRow = exactRow;
      this.rowPrefix = rowPrefix;
      this.help = help;
      this.hexEncNonAscii = hexEncNonAscii;
      this.scanAccumuloTable = scanAccumuloTable;
      this.exportAsJson = exportAsJson;
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
