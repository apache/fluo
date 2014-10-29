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
package io.fluo.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TransactionImpl;
import io.fluo.core.util.SpanUtil;
import org.apache.accumulo.core.client.mapreduce.AccumuloInputFormat;
import org.apache.accumulo.core.client.mapreduce.RangeInputSplit;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * This input format reads a consistent snapshot from a Fluo table.
 */
public class FluoInputFormat extends InputFormat<Bytes,ColumnIterator> {
  
  private static String TIMESTAMP_CONF_KEY = FluoInputFormat.class.getName() + ".timestamp";
  private static String PROPS_CONF_KEY = FluoInputFormat.class.getName() + ".props";
  private static String FAMS_CONF_KEY = FluoInputFormat.class.getName() + ".families";

  @Override
  public RecordReader<Bytes,ColumnIterator> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

    return new RecordReader<Bytes,ColumnIterator>() {
      
      private Entry<Bytes,ColumnIterator> entry;
      private RowIterator rowIter;
      private Environment env = null;
      private TransactionImpl ti = null;
      
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
      public Bytes getCurrentKey() throws IOException, InterruptedException {
        return entry.getKey();
      }
      
      @Override
      public ColumnIterator getCurrentValue() throws IOException, InterruptedException {
        return entry.getValue();
      }
      
      @Override
      public float getProgress() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return 0;
      }
      
      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        try {
          // TODO this uses non public Accumulo API!
          RangeInputSplit ris = (RangeInputSplit) split;
          
          Span span = SpanUtil.toSpan(ris.getRange());
          
          ByteArrayInputStream bais = new ByteArrayInputStream(context.getConfiguration().get(PROPS_CONF_KEY).getBytes("UTF-8"));
          PropertiesConfiguration props = new PropertiesConfiguration();
          props.load(bais);
          
          env = new Environment(new FluoConfiguration(props));
          
          ti = new TransactionImpl(env, context.getConfiguration().getLong(TIMESTAMP_CONF_KEY, -1));
          ScannerConfiguration sc = new ScannerConfiguration().setSpan(span);
          
          for (String fam : context.getConfiguration().getStrings(FAMS_CONF_KEY, new String[0]))
            sc.fetchColumnFamily(Bytes.wrap(fam));

          rowIter = ti.get(sc);
        } catch (Exception e) {
          throw new IOException(e);
        }
      }
      
      @Override
      public boolean nextKeyValue() throws IOException, InterruptedException {
        if (rowIter.hasNext()) {
          entry = rowIter.next();
          return true;
        }
        return false;
      }
    };

  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
    return new AccumuloInputFormat().getSplits(context);
  }
  
  /**
   * Configure properties needed to connect to a Fluo instance
   * 
   * @param conf
   * @param props
   *          use {@link io.fluo.api.config.FluoConfiguration} to configure programmatically
   */
  @SuppressWarnings("deprecation")
  public static void configure(Job conf, Properties props) {
    try {
      FluoConfiguration config = new FluoConfiguration(ConfigurationConverter.getConfiguration(props));
      try (Environment env = new Environment(config)) {
        long ts = env.getSharedResources().getTimestampTracker().allocateTimestamp();
        conf.getConfiguration().setLong(TIMESTAMP_CONF_KEY, ts);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        props.store(baos, "");
        conf.getConfiguration().set(PROPS_CONF_KEY, new String(baos.toByteArray(), "UTF8"));

        AccumuloInputFormat.setZooKeeperInstance(conf, config.getAccumuloInstance(), config.getZookeepers());
        AccumuloInputFormat.setConnectorInfo(conf, config.getAccumuloUser(), new PasswordToken(config.getAccumuloPassword()));
        AccumuloInputFormat.setInputTableName(conf, env.getTable());
        AccumuloInputFormat.setScanAuthorizations(conf, env.getAuthorizations());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  // TODO support text
  public static void fetchFamilies(Job job, String... fams) {
    job.getConfiguration().setStrings(FAMS_CONF_KEY, fams);
  }

  public static void fetchFamilies(Job job, Bytes... fams) {
    // TODO support binary data
    String sfams[] = new String[fams.length];
    for (int i = 0; i < sfams.length; i++) {
      sfams[i] = fams[i].toString();
    }
    fetchFamilies(job, sfams);
  }

  // TODO let user set auths
  // TODO let user set ranges
}
