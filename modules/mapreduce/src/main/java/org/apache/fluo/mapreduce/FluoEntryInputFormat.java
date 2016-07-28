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

package org.apache.fluo.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.impl.TransactionImpl;
import org.apache.fluo.core.util.SpanUtil;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This input format reads a consistent snapshot of Fluo entries from a Fluo table.
 */
public class FluoEntryInputFormat extends InputFormat<RowColumn, Bytes> {

  private static String TIMESTAMP_CONF_KEY = FluoEntryInputFormat.class.getName() + ".timestamp";
  private static String PROPS_CONF_KEY = FluoEntryInputFormat.class.getName() + ".props";
  private static String FAMS_CONF_KEY = FluoEntryInputFormat.class.getName() + ".families";

  @Override
  public RecordReader<RowColumn, Bytes> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {

    return new RecordReader<RowColumn, Bytes>() {

      private RowColumnValue rowColVal;
      private Environment env = null;
      private TransactionImpl ti = null;
      private Iterator<RowColumnValue> cellIterator;

      @Override
      public void close() throws IOException {
        if (ti != null) {
          ti.close();
        }

        if (env != null) {
          env.close();
        }
      }

      @Override
      public RowColumn getCurrentKey() throws IOException, InterruptedException {
        return rowColVal.getRowColumn();
      }

      @Override
      public Bytes getCurrentValue() throws IOException, InterruptedException {
        return rowColVal.getValue();
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return 0;
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
          InterruptedException {
        try {
          ByteArrayInputStream bais =
              new ByteArrayInputStream(context.getConfiguration().get(PROPS_CONF_KEY)
                  .getBytes(StandardCharsets.UTF_8));

          env = new Environment(new FluoConfiguration(bais));

          ti = new TransactionImpl(env, context.getConfiguration().getLong(TIMESTAMP_CONF_KEY, -1));

          // TODO this uses non public Accumulo API!
          RangeInputSplit ris = (RangeInputSplit) split;
          Span span = SpanUtil.toSpan(ris.getRange());

          HashSet<Column> columns = new HashSet<>();

          for (String fam : context.getConfiguration().getStrings(FAMS_CONF_KEY, new String[0])) {
            columns.add(new Column(fam));
          }

          cellIterator = ti.scanner().over(span).fetch(columns).build().iterator();
        } catch (Exception e) {
          throw new IOException(e);
        }
      }

      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (cellIterator.hasNext()) {
          rowColVal = cellIterator.next();
          return true;
        } else {
          rowColVal = null;
          return false;
        }
      }
    };
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return new AccumuloInputFormat().getSplits(context);
  }

  /**
   * Configure properties needed to connect to a Fluo application
   *
   * @param conf Job configuration
   * @param config use {@link FluoConfiguration} to configure programmatically
   */
  @SuppressWarnings("deprecation")
  public static void configure(Job conf, SimpleConfiguration config) {
    try {
      FluoConfiguration fconfig = new FluoConfiguration(config);
      try (Environment env = new Environment(fconfig)) {
        long ts =
            env.getSharedResources().getTimestampTracker().allocateTimestamp().getTxTimestamp();
        conf.getConfiguration().setLong(TIMESTAMP_CONF_KEY, ts);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        config.save(baos);
        conf.getConfiguration().set(PROPS_CONF_KEY,
            new String(baos.toByteArray(), StandardCharsets.UTF_8));

        AccumuloInputFormat.setZooKeeperInstance(conf, fconfig.getAccumuloInstance(),
            fconfig.getAccumuloZookeepers());
        AccumuloInputFormat.setConnectorInfo(conf, fconfig.getAccumuloUser(), new PasswordToken(
            fconfig.getAccumuloPassword()));
        AccumuloInputFormat.setInputTableName(conf, env.getTable());
        AccumuloInputFormat.setScanAuthorizations(conf, env.getAuthorizations());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void fetchFamilies(Job job, String... fams) {
    job.getConfiguration().setStrings(FAMS_CONF_KEY, fams);
  }

  public static void fetchFamilies(Job job, Bytes... fams) {
    // TODO support binary data
    String[] sfams = new String[fams.length];
    for (int i = 0; i < sfams.length; i++) {
      sfams[i] = fams[i].toString();
    }
    fetchFamilies(job, sfams);
  }

  // TODO let user set auths
  // TODO let user set ranges
}
