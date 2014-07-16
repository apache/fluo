package io.fluo.api.mapreduce;

import java.io.IOException;

import io.fluo.impl.WriteValue;
import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import io.fluo.impl.ColumnUtil;

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

public class FluoFileOutputFormat extends FileOutputFormat<RowColumn,ByteSequence> {

  @Override
  public RecordWriter<RowColumn,ByteSequence> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
    final RecordWriter<Key,Value> accumuloRecordWriter = new AccumuloFileOutputFormat().getRecordWriter(job);

    return new RecordWriter<RowColumn,ByteSequence>() {

      @Override
      public void write(RowColumn key, ByteSequence value) throws IOException, InterruptedException {
        byte[] row = key.getRow().toArray();
        byte[] fam = key.getColumn().getFamily().toArray();
        byte[] qual = key.getColumn().getQualifier().toArray();
        byte[] vis = key.getColumn().getVisibility().getExpression();

        Key dataKey = new Key(row, fam, qual, vis, ColumnUtil.DATA_PREFIX | 0, false);
        Key writeKey = new Key(row, fam, qual, vis, ColumnUtil.WRITE_PREFIX | 1, false);

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
