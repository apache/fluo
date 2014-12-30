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

import io.fluo.api.client.Snapshot;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.stress.trie.Constants;
import io.fluo.stress.trie.Node;
import io.fluo.stress.trie.NodeObserver;
import org.apache.commons.configuration.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class TrieStopLevelIT extends TrieMapRedIT {
  
  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(NodeObserver.class.getName()));
  }

  @Override
  protected void setAppConfig(Configuration config){
    config.setProperty(Constants.STOP_LEVEL_PROP, 7);
    config.setProperty(Constants.NODE_SIZE_PROP, 8);
  }
  
  @Test
  public void testEndToEnd() throws Exception {
    super.testEndToEnd();
    try(Snapshot snap = client.newSnapshot()){
      Bytes row = Bytes.of(Node.generateRootId(8));
      Assert.assertNull(snap.get(row, Constants.COUNT_SEEN_COL));
      Assert.assertNull(snap.get(row, Constants.COUNT_WAIT_COL));
    }
  }
}
