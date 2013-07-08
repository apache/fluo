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
package org.apache.accumulo.accismus.benchmark;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.accismus.Column;
import org.apache.accumulo.accismus.Observer;
import org.apache.accumulo.accismus.Transaction;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

/**
 * 
 */
public class ClusterIndexer  implements Observer  {

  static final Column key1Col = new Column("key", "1");
  static final Column key2Col = new Column("key", "2");
  static final Column key3Col = new Column("key", "3");
  static final Set<Column> keyColumns = new HashSet<Column>(Arrays.asList(key1Col, key2Col, key3Col));
  static final ByteSequence DUP = new ArrayByteSequence("dup");
  static final ByteSequence EMPTY = new ArrayByteSequence("");

  @Override
  public void process(Transaction tx, ByteSequence url, Column col) throws Exception {
    ByteSequence contents = tx.get(url, col);
    
    Document doc = new Document(url, contents);
    
    Map<Column,ByteSequence> keys = tx.get(url, keyColumns);
    
    for (Entry<Column,ByteSequence> entry : keys.entrySet()) {
      tx.delete(entry.getValue(), new Column(DUP, url));
    }
    
    // these are set in this transaction to keep track of whats indexed. If set when the document was inserted, then would not match whats
    // indexed.
    tx.set(url, key1Col, doc.getKey1());
    tx.set(url, key2Col, doc.getKey2());
    tx.set(url, key3Col, doc.getKey3());
    
    tx.set(doc.getKey1(), new Column(DUP, url), EMPTY);
    tx.set(doc.getKey2(), new Column(DUP, url), EMPTY);
    tx.set(doc.getKey3(), new Column(DUP, url), EMPTY);
  }
  
}
