package org.fluo.impl;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.MutationsRejectedException;

public class SharedResources {

  private BatchWriter bw;
  private ConditionalWriter cw;
  private SharedBatchWriter sbw;

  SharedResources(BatchWriter bw, ConditionalWriter cw) {
    this.sbw = new SharedBatchWriter(bw);
    this.bw = bw;
    this.cw = cw;
  }

  public SharedBatchWriter getBatchWriter() {
    return sbw;
  }
  
  public ConditionalWriter getConditionalWriter(){
    return cw;
  }
  
  public void close(){
    sbw.close();
    cw.close();
    try {
      bw.close();
    } catch (MutationsRejectedException e) {
      throw new RuntimeException(e);
    }
    
  }
}
