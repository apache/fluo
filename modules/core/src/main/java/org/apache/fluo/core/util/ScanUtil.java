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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonIOException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
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
  public static final String CSV_HEADER = "csv.header";
  public static final String CSV_QUOTE_MODE = "csv.quoteMode";
  public static final String CSV_QUOTE = "csv.quote";
  public static final String CSV_ESCAPE = "csv.escape";
  public static final String CSV_DELIMITER = "csv.delimiter";
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

  public static void scanFluo(ScanOpts options, FluoConfiguration sConfig, PrintStream out)
      throws IOException {

    try (FluoClient client = FluoFactory.newClient(sConfig)) {
      try (Snapshot s = client.newSnapshot()) {

        Span span = getSpan(options);
        Collection<Column> columns = getColumns(options);

        if (options.exportAsJson) {
          generateJson(options, span, columns, s, out);
        } else { // TSV or CSV format
          generateTsvCsv(options, span, columns, s, out);
        }

      } catch (FluoException e) {
        throw e;
      }
    }
  }

  /**
   * Generate TSV or CSV format as result of the scan.
   * 
   * @since 1.2
   */
  private static void generateTsvCsv(ScanOpts options, Span span, Collection<Column> columns,
      final Snapshot snapshot, PrintStream out) throws IOException {
    // CSV Formater
    CSVFormat csvFormat = CSVFormat.DEFAULT;
    csvFormat = csvFormat.withQuoteMode(QuoteMode.ALL);
    csvFormat = csvFormat.withRecordSeparator("\n");

    // when "--csv" parameter is passed the "fluo.scan.csv" is analised
    if (options.exportAsCsv) {
      if (StringUtils.isNotEmpty(options.csvDelimiter)) {
        if (options.csvDelimiter.length() > 1) {
          throw new IllegalArgumentException(
              "Invalid character for the \"--csv-delimiter\" parameter.");
        }
        csvFormat = csvFormat.withDelimiter(options.csvDelimiter.charAt(0));
      }

      if (StringUtils.isNotEmpty(options.csvEscape)) {
        if (options.csvEscape.length() > 1) {
          throw new IllegalArgumentException(
              "Invalid character for the \"--csv-escape\" parameter.");
        }
        csvFormat = csvFormat.withEscape(options.csvEscape.charAt(0));
      }

      if (StringUtils.isNotEmpty(options.csvQuote)) {
        if (options.csvQuote.length() > 1) {
          throw new IllegalArgumentException(
              "Invalid character for the \"--csv-quote\" parameter.");
        }
        csvFormat = csvFormat.withQuote(options.csvQuote.charAt(0));
      }

      // It can throw "java.lang.IllegalArgumentException" if the value not exists
      // in "org.apache.commons.csv.QuoteMode"
      if (StringUtils.isNotEmpty(options.csvQuoteMode)) {
        csvFormat = csvFormat.withQuoteMode(QuoteMode.valueOf(options.csvQuoteMode));
      }

      if (BooleanUtils.toBooleanObject(
          ObjectUtils.defaultIfNull(options.csvHeader, Boolean.FALSE.toString()))) {
        csvFormat = csvFormat.withHeader(FLUO_ROW, FLUO_COLUMN_FAMILY, FLUO_COLUMN_QUALIFIER,
            FLUO_COLUMN_VISIBILITY, FLUO_VALUE);
      }
    } else {
      // Default TAB separator and NO quotes if possible.
      csvFormat = csvFormat.withDelimiter(CSVFormat.TDF.getDelimiter());
      csvFormat = csvFormat.withQuoteMode(QuoteMode.MINIMAL);
    }

    try (CSVPrinter printer = new CSVPrinter(out, csvFormat)) {
      CellScanner cellScanner = snapshot.scanner().over(span).fetch(columns).build();

      List<Object> record = new LinkedList<>();
      StringBuilder sb = new StringBuilder();
      int lines2check = 0;
      for (RowColumnValue rcv : cellScanner) {
        record.clear();
        if (options.hexEncNonAscii) {
          sb.setLength(0);
          Hex.encNonAscii(sb, rcv.getRow());
          record.add(sb.toString());
          sb.setLength(0);
          Hex.encNonAscii(sb, rcv.getColumn().getFamily());
          record.add(sb.toString());
          sb.setLength(0);
          Hex.encNonAscii(sb, rcv.getColumn().getQualifier());
          record.add(sb.toString());
          sb.setLength(0);
          Hex.encNonAscii(sb, rcv.getColumn().getVisibility());
          record.add(sb.toString());
          sb.setLength(0);
          Hex.encNonAscii(sb, rcv.getValue());
          record.add(sb.toString());
        } else {
          record.add(rcv.getsRow());
          record.add(rcv.getColumn().getFamily());
          record.add(rcv.getColumn().getQualifier());
          record.add(rcv.getColumn().getVisibility());
          record.add(rcv.getsValue());
        }

        printer.printRecord(record);
        lines2check++;
        if (lines2check == 100) {
          lines2check = 0;
          if (out.checkError()) {
            throw new IOException("Fail to write data to stream.");
          }
        }
      }
    }
    out.flush();
  }

  /**
   * Generate JSON format as result of the scan.
   * 
   * @since 1.2
   */
  private static void generateJson(ScanOpts options, Span span, Collection<Column> columns,
      final Snapshot snapshot, PrintStream out) throws JsonIOException {
    Gson gson = new GsonBuilder().serializeNulls().setDateFormat(DateFormat.LONG)
        .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES).setVersion(1.0)
        .create();

    CellScanner cellScanner = snapshot.scanner().over(span).fetch(columns).build();

    StringBuilder sb = new StringBuilder();
    Map<String, String> json = new LinkedHashMap<>();
    for (RowColumnValue rcv : cellScanner) {
      sb.setLength(0);
      Hex.encNonAscii(sb, rcv.getRow());
      json.put(FLUO_ROW, sb.toString());
      sb.setLength(0);
      Hex.encNonAscii(sb, rcv.getColumn().getFamily());
      json.put(FLUO_COLUMN_FAMILY, sb.toString());
      Hex.encNonAscii(sb, rcv.getColumn().getQualifier());
      json.put(FLUO_COLUMN_QUALIFIER, sb.toString());
      sb.setLength(0);
      Hex.encNonAscii(sb, rcv.getColumn().getVisibility());
      json.put(FLUO_COLUMN_VISIBILITY, sb.toString());
      sb.setLength(0);
      Hex.encNonAscii(sb, rcv.getValue());
      json.put(FLUO_VALUE, sb.toString());

      gson.toJson(json, out);
      out.append("\n");
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
    public boolean exportAsCsv = false;
    public boolean exportAsJson = false;
    public final String csvDelimiter;
    public final String csvEscape;
    public final String csvHeader;
    public final String csvQuote;
    public final String csvQuoteMode;

    public ScanOpts(String startRow, String endRow, List<String> columns, String exactRow,
        String rowPrefix, boolean help, boolean hexEncNonAscii, boolean scanAccumuloTable,
        boolean exportAsCsv, String csvDelimiter, String csvEscape, String csvHeader,
        String csvQuote, String csvQuoteMode, boolean exportAsJson) {
      this.startRow = startRow;
      this.endRow = endRow;
      this.columns = columns;
      this.exactRow = exactRow;
      this.rowPrefix = rowPrefix;
      this.help = help;
      this.hexEncNonAscii = hexEncNonAscii;
      this.scanAccumuloTable = scanAccumuloTable;
      this.exportAsCsv = exportAsCsv;
      this.csvDelimiter = csvDelimiter;
      this.csvEscape = csvEscape;
      this.csvHeader = csvHeader;
      this.csvQuote = csvQuote;
      this.csvQuoteMode = csvQuoteMode;
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
