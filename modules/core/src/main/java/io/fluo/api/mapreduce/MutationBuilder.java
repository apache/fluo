package io.fluo.api.mapreduce;

import io.fluo.api.Bytes;
import io.fluo.api.Column;
import io.fluo.impl.ByteUtil;
import io.fluo.impl.ColumnUtil;
import io.fluo.impl.WriteValue;

import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;

/**
 * This class allows building Accumulo mutations that are in the Fluo data format. This class is intended to be used with {@link AccumuloOutputFormat}
 * inorder to seed an initialized Fluo table on which no transactions have executed.
 */

public class MutationBuilder {

  private Mutation mutation;

  public MutationBuilder(Bytes row) {
    if (row.isBackedByArray())
      mutation = new Mutation(row.getBackingArray(), row.offset(), row.length());
    else
      mutation = new Mutation(row.toArray());
  }

  public MutationBuilder put(Column col, Bytes value) {

    Text fam = ByteUtil.toText(col.getFamily());
    Text qual = ByteUtil.toText(col.getQualifier());

    mutation.put(fam, qual, col.getVisibilityParsed(), ColumnUtil.DATA_PREFIX | 0, new Value(value.toArray()));
    mutation.put(fam, qual, col.getVisibilityParsed(), ColumnUtil.WRITE_PREFIX | 1, new Value(WriteValue.encode(0, false, false)));

    return this;
  }

  Mutation build() {
    Mutation ret = mutation;
    mutation = null;
    return ret;
  }
}
