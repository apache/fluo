package io.fluo.api.mapreduce;

import io.fluo.api.Column;
import io.fluo.impl.ColumnUtil;
import io.fluo.impl.WriteValue;
import org.apache.accumulo.core.client.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Mutation;

/**
 * This class allows building Accumulo mutations that are in the Fluo data format. This class is intended to be used with {@link AccumuloOutputFormat}
 * inorder to seed an initialized Fluo table on which no transactions have executed.
 */

public class MutationBuilder {

  private Mutation mutation;

  public MutationBuilder(ByteSequence row) {
    if (row.isBackedByArray())
      mutation = new Mutation(row.getBackingArray(), row.offset(), row.length());
    else
      mutation = new Mutation(row.toArray());
  }

  public MutationBuilder put(Column col, ByteSequence value) {

    byte[] fam = col.getFamily().toArray();
    byte[] qual = col.getQualifier().toArray();

    mutation.put(fam, qual, col.getVisibility(), ColumnUtil.DATA_PREFIX | 0, value.toArray());
    mutation.put(fam, qual, col.getVisibility(), ColumnUtil.WRITE_PREFIX | 1, WriteValue.encode(0, false, false));

    return this;
  }

  Mutation build() {
    Mutation ret = mutation;
    mutation = null;
    return ret;
  }
}
