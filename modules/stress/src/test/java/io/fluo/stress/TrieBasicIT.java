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
package io.fluo.stress;

import static io.fluo.stress.trie.Constants.COUNT_SEEN_COL;
import static io.fluo.stress.trie.Constants.TYPEL;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fluo.api.LoaderExecutor;
import io.fluo.api.SnapshotFactory;
import io.fluo.api.config.LoaderExecutorProperties;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.stress.trie.Node;
import io.fluo.stress.trie.NodeObserver;
import io.fluo.stress.trie.NumberLoader;

/** Tests Trie Stress Test using Basic Loader
 */
public class TrieBasicIT extends StressBase {
  
  private static Logger log = LoggerFactory.getLogger(TrieBasicIT.class);
  
  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(NodeObserver.class.getName()));
  }
  
  @Test
  public void testBit32() throws Exception {
    runTrieTest(20, Integer.MAX_VALUE, 32);
  }
  
  @Test
  public void testBit8() throws Exception {
    runTrieTest(25, Integer.MAX_VALUE, 8);
  }
  
  @Test
  public void testBit4() throws Exception {
    runTrieTest(10, Integer.MAX_VALUE, 4);
  }
  
  @Test
  public void testBit() throws Exception {
    runTrieTest(5, Integer.MAX_VALUE, 1);
  }
  
  @Test
  public void testDuplicates() throws Exception {
    runTrieTest(20, 10, 4);
  }
    
  private void runTrieTest(int ingestNum, int maxValue, int nodeSize) throws Exception {
    
    log.info("Ingesting "+ingestNum+" unique numbers with a nodeSize of "+nodeSize+" bits");
    
    LoaderExecutorProperties lep = new LoaderExecutorProperties(props);
    lep.setNumThreads(0);
    lep.setQueueSize(0);
    
    LoaderExecutor le = new LoaderExecutor(lep);
    
    Random random = new Random();
    Set<Integer> ingested = new HashSet<>();
    for (int i = 0; i < ingestNum; i++) {
      int num = Math.abs(random.nextInt(maxValue));
      le.execute(new NumberLoader(num, nodeSize));
      ingested.add(num);
    }
    int uniqueNum = ingested.size();
    
    log.info("Ingested "+uniqueNum+" unique numbers with a nodeSize of "+nodeSize+" bits");
    
    miniFluo.waitForObservers();
    
    try (SnapshotFactory snapFact = new SnapshotFactory(props)) {          
      TypedSnapshot tsnap = TYPEL.snapshot(snapFact);
      
      Integer result = tsnap.get().row(Node.generateRootId(nodeSize))
                            .col(COUNT_SEEN_COL).toInteger();
      if (result == null) { 
        log.error("Could not find root node");
        printSnapshot();
      }
      
      if (!result.equals(uniqueNum)) {
        log.error("Count ("+result+") at root node does not match expected ("+uniqueNum+"):");
        printSnapshot();
      }
      
      Assert.assertEquals(uniqueNum, result.intValue());;
    }
  }
}
