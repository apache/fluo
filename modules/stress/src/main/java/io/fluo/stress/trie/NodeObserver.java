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

import org.apache.accumulo.core.data.ByteSequence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fluo.api.AbstractObserver;
import io.fluo.api.Column;
import io.fluo.api.Transaction;
import io.fluo.api.types.TypedTransaction;

/** Observer that looks for count:wait for nodes.  If found,
 * it increments count:seen and increments count:wait of parent
 * node in trie
 */
public class NodeObserver extends AbstractObserver {
  
  private static Logger log = LoggerFactory.getLogger(NodeObserver.class);

  @Override
  public void process(Transaction tx, ByteSequence row, Column col) throws Exception {
    
    TypedTransaction ttx = Constants.TYPEL.transaction(tx);
    Integer childWait = ttx.get().row(row).col(Constants.COUNT_WAIT_COL).toInteger(0);
    
    if (childWait > 0) {
      Integer childSeen = ttx.get().row(row).col(Constants.COUNT_SEEN_COL).toInteger(0);

      ttx.mutate().row(row).col(Constants.COUNT_SEEN_COL).set(childSeen + childWait);
      ttx.mutate().row(row).col(Constants.COUNT_WAIT_COL).delete();
      
      try {
        Node node = new Node(row.toString());
        if (node.isRoot() == false) {
          Node parent = node.getParent();
          Integer parentWait = ttx.get().row(parent.getRowId()).col(Constants.COUNT_WAIT_COL).toInteger(0);
          ttx.mutate().row(parent.getRowId()).col(Constants.COUNT_WAIT_COL).set(parentWait + childWait);
        }
      } catch (IllegalArgumentException e) {
        log.error(e.getMessage());
        e.printStackTrace();
      } 
    }
  }
    
  @Override
  public ObservedColumn getObservedColumn() {
    return new ObservedColumn(Constants.COUNT_WAIT_COL, NotificationType.STRONG);
  }
}
