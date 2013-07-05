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
package org.apache.accumulo.accismus;

import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.accismus.benchmark.Document;
import org.apache.accumulo.accismus.benchmark.Generator;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.junit.Test;

/**
 * 
 */
public class BenchTest extends TestBase {
  @Test
  public void test1() throws Exception {
    Random rand = new Random();
    
    for (int i = 0; i < 10; i++) {
      Document doc = new Document(rand);
      
      Generator.insert(config, doc);
    }
    
    Transaction tx = new Transaction(config);
    
    RowIterator rowIter = tx.get(new ScannerConfiguration().fetchColumnFamily(new ArrayByteSequence("dup")).fetchColumnFamily(new ArrayByteSequence("key")));
    while (rowIter.hasNext()) {
      Entry<ByteSequence,ColumnIterator> cols = rowIter.next();
      ColumnIterator colIiter = cols.getValue();
      while (colIiter.hasNext()) {
        Entry<Column,ByteSequence> cv = colIiter.next();
        System.out.println(cols.getKey() + " " + cv.getKey() + " " + cv.getValue());
      }
    }

  }
}
