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

import java.io.IOException;

import io.fluo.accumulo.util.ColumnConstants;
import io.fluo.accumulo.values.WriteValue;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.core.util.ByteUtil;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This class wraps the {@link AccumuloFileOutputFormat} and converts to Fluo's data format. You can use the static methods on
 * {@link AccumuloFileOutputFormat} to configure the output file.
 *
 * The intended use of this output format is for seeding an initialized Fluo table on which no transactions have executed.
 *
 * As with the Accumulo file output format, rows and columns must be written in sorted order.
 *
 * For writing data with {@link AccumuloOutputFormat}, see {@link MutationBuilder}
 */

public class FluoFileOutputFormat extends FileOutputFormat<RowColumn,Bytes> {

  @Override
  public RecordWriter<RowColumn,Bytes> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    final RecordWriter<Key,Value> accumuloRecordWriter = new AccumuloFileOutputFormat().getRecordWriter(job);

    return new RecordWriter<RowColumn,Bytes>() {

      @Override
      public void write(RowColumn key, Bytes value) throws IOException, InterruptedException {
        Text row = ByteUtil.toText(key.getRow());
        Text fam = ByteUtil.toText(key.getColumn().getFamily());
        Text qual = ByteUtil.toText(key.getColumn().getQualifier());
        Text vis = ByteUtil.toText(key.getColumn().getVisibility());

        Key dataKey = new Key(row, fam, qual, vis, ColumnConstants.DATA_PREFIX | 0);
        Key writeKey = new Key(row, fam, qual, vis, ColumnConstants.WRITE_PREFIX | 1);

        accumuloRecordWriter.write(writeKey, new Value(WriteValue.encode(0, false, false)));
        accumuloRecordWriter.write(dataKey, new Value(value.toArray()));
      }

      @Override
      public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        accumuloRecordWriter.close(context);
      }
    };
  }

}
