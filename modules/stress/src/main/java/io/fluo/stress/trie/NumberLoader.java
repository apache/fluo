/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.stress.trie;

import static com.google.common.base.Preconditions.checkArgument;

import io.fluo.api.client.Loader;

import io.fluo.api.client.Transaction;
import io.fluo.api.types.TypedTransaction;

/** Executes load transactions of numbers into trie at leaf node level
 */
public class NumberLoader implements Loader {
  
  private Number number;
  private int level;
  private int nodeSize;
  
  public NumberLoader(Integer num, int nodeSize) {
    checkArgument(num >= 0, "Only positive numbers accepted");
    checkArgument((nodeSize <= 32) && ((32 % nodeSize) == 0), "nodeSize must be divisor of 32"); 
    this.number = num;
    this.nodeSize = nodeSize;
    this.level = 32 / nodeSize;
  }
  
  public NumberLoader(Long num, int nodeSize) {
    checkArgument(num >= 0, "Only positive numbers accepted");
    checkArgument((nodeSize <= 64) && ((64 % nodeSize) == 0), "nodeSize must be divisor of 64");
    this.number = num;
    this.nodeSize = nodeSize;
    this.level = 64 / nodeSize;
  }
  
  @Override
  public void load(Transaction tx) throws Exception {
    
    TypedTransaction ttx = Constants.TYPEL.transaction(tx);
    
    String rowId = new Node(number, level, nodeSize).getRowId();
        
    Integer seen = ttx.get().row(rowId).col(Constants.COUNT_SEEN_COL).toInteger(0);
    if (seen == 0) {
      Integer wait = ttx.get().row(rowId).col(Constants.COUNT_WAIT_COL).toInteger(0);
      if (wait == 0) {
        ttx.mutate().row(rowId).col(Constants.COUNT_WAIT_COL).set(1);
      }
    }
  }
}
