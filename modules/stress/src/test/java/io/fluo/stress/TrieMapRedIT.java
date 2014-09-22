/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.stress;

import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;

import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.types.TypedSnapshot;
import io.fluo.core.TestBaseMini;
import io.fluo.stress.trie.Node;
import io.fluo.stress.trie.NodeObserver;
import io.fluo.stress.trie.NumberIngest;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.PipelineMapReduceDriver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.fluo.stress.trie.Constants.COUNT_SEEN_COL;
import static io.fluo.stress.trie.Constants.TYPEL;

/** 
 * Tests Trie Stress Test using MapReduce Ingest
 */
public class TrieMapRedIT extends TestBaseMini {
  
  private static final Logger log = LoggerFactory.getLogger(TrieMapRedIT.class);
  private PipelineMapReduceDriver<LongWritable, Text, Text, LongWritable> driver;
  
  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(NodeObserver.class.getName()));
  }
  
  @Before
  @SuppressWarnings("resource")
  // resources closed by driver
  public void setUp() {
    driver = PipelineMapReduceDriver.newPipelineMapReduceDriver();
    driver.addMapReduce(new NumberIngest.IngestMapper(), new NumberIngest.UniqueReducer());
    driver.addMapReduce(new NumberIngest.CountMapper(), new NumberIngest.CountReducer());
  }
  
  @Test
  public void testIngest() throws Exception {
    runMapRedTest(2, 10, 4);
  }
  
  public void runMapRedTest(Integer mappers, Integer numPerMapper, Integer nodeSize) throws Exception {
    
    Configuration driverConfig = driver.getConfiguration();
    driverConfig.setInt(NumberIngest.TRIE_NODE_SIZE_PROP, nodeSize);
    loadConfig(driverConfig, ConfigurationConverter.getProperties(config));
    
    Integer total = mappers * numPerMapper;
    for (int i=0; i < mappers; i++) {
      driver.addInput(new LongWritable(i), new Text(numPerMapper.toString()));
    }
    driver.withOutput(new Text("COUNT"), new LongWritable(total));
    driver.runTest();
    
    // TODO - If sleep is removed test sometimes fails
    Thread.sleep(4000);

    miniFluo.waitForObservers();

    try (TypedSnapshot tsnap = TYPEL.wrap(client.newSnapshot())) {
      Integer result = tsnap.get().row(Node.generateRootId(nodeSize)).col(COUNT_SEEN_COL).toInteger();
      if (result == null) {
        log.error("Could not find root node");
      } else if (!result.equals(total)) {
        log.error("Count (" + result + ") at root node does not match expected (" + total + "):");
      }
      Assert.assertEquals(total.intValue(), result.intValue());
    }
  }
  
  private static void loadConfig(Configuration conf, Properties props) {
    for (Entry<Object, Object> entry : props.entrySet()) {
      conf.set((String)entry.getKey(), (String)entry.getValue());
    }
  }
}
