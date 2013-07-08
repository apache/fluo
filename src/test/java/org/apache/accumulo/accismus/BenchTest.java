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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.apache.accumulo.accismus.benchmark.ClusterIndexer;
import org.apache.accumulo.accismus.benchmark.Document;
import org.apache.accumulo.accismus.benchmark.Generator;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class BenchTest extends TestBase {
  
  protected Map<Column,Class<? extends Observer>> getObservers() {
    Map<Column,Class<? extends Observer>> colObservers = new HashMap<Column,Class<? extends Observer>>();
    colObservers.put(Generator.contetCol, ClusterIndexer.class);
    return colObservers;
  }

  @Test
  public void test1() throws Exception {
    
    Map<ByteSequence,Document> expected = new HashMap<ByteSequence,Document>();

    Random rand = new Random();
    
    for (int i = 0; i < 10; i++) {
      Document doc = new Document(rand);
      expected.put(doc.getUrl(), doc);
      Generator.insert(config, doc);
    }
    
    runWorker();

    verify(expected);
    
    // update a document
    ByteSequence uri = expected.keySet().iterator().next();
    Random r = new Random();
    byte newContent[] = new byte[1004];
    r.nextBytes(newContent);
    Document newDoc = new Document(uri, new ArrayByteSequence(newContent));
    
    Generator.insert(config, newDoc);

    expected.put(uri, newDoc);

    runWorker();
    
    verify(expected);
    
    runWorker();
  }

  /**
   * @param expected
   * 
   */
  private void verify(Map<ByteSequence,Document> expected) throws Exception {
    Transaction tx1 = new Transaction(config);
    
    RowIterator riter = tx1.get(new ScannerConfiguration());
    
    HashSet<ByteSequence> docsSeen = new HashSet<ByteSequence>();

    HashSet<ByteSequence> docsSeenK1 = new HashSet<ByteSequence>();
    HashSet<ByteSequence> docsSeenK2 = new HashSet<ByteSequence>();
    HashSet<ByteSequence> docsSeenK3 = new HashSet<ByteSequence>();

    while (riter.hasNext()) {
      Entry<ByteSequence,ColumnIterator> cols = riter.next();
      String row = cols.getKey().toString();
      
      if (row.startsWith("ke")) {
        ColumnIterator citer = cols.getValue();
        while (citer.hasNext()) {
          Entry<Column,ByteSequence> cv = citer.next();
          
          Document doc = expected.get(cv.getKey().getQualifier());
          
          ByteSequence ek = null;
          if (row.startsWith("ke1")) {
            ek = doc.getKey1();
            docsSeenK1.add(cv.getKey().getQualifier());
          } else if (row.startsWith("ke2")) {
            ek = doc.getKey2();
            docsSeenK2.add(cv.getKey().getQualifier());
          } else if (row.startsWith("ke3")) {
            ek = doc.getKey3();
            docsSeenK3.add(cv.getKey().getQualifier());
          }
          
          Assert.assertEquals(ek, cols.getKey());

        }
      } else {
        Assert.assertTrue(docsSeen.add(cols.getKey()));
      }
    }

    Assert.assertEquals(expected.keySet(), docsSeen);
    
    Assert.assertEquals(expected.keySet(), docsSeenK1);
    Assert.assertEquals(expected.keySet(), docsSeenK2);
    Assert.assertEquals(expected.keySet(), docsSeenK3);
  }
  
  // TODO test deleting document
}
