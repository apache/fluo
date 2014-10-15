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

package io.fluo.stress.trie;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.api.types.TypedSnapshot;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Print {

  private static final Logger log = LoggerFactory.getLogger(Print.class);

  public static class Stats {
    public int totalWait = 0;
    public int rootValue = 0;
    public boolean sawOtherNodes = false;
    public Map<Integer,Integer> levelWait = new TreeMap<>();
    public Map<Integer,Integer> nodesPerLevel = new TreeMap<>();

    public Stats() {

    }

    public Stats(int tw, int rv, boolean son) {
      this.totalWait = tw;
      this.rootValue = rv;
      this.sawOtherNodes = son;
    }

    public boolean equals(Object o) {
      if (o instanceof Stats) {
        Stats os = (Stats) o;

        return totalWait == os.totalWait && rootValue == os.rootValue && sawOtherNodes == os.sawOtherNodes && levelWait.equals(os.levelWait)
            && nodesPerLevel.equals(os.nodesPerLevel);
      }

      return false;
    }

    public void incrementLevelWait(int level, int amt) {
      Integer current = levelWait.get(level);
      if (current == null)
        current = 0;
      levelWait.put(level, current + amt);

      current = nodesPerLevel.get(level);
      if (current == null)
        current = 0;
      nodesPerLevel.put(level, current + 1);
    }

  }

  public static Stats getStats(Configuration config, int nodeSize) throws Exception {

    try (FluoClient client = FluoFactory.newClient(config); Snapshot snap = client.newSnapshot()) {

      ScannerConfiguration scanConfig = new ScannerConfiguration();
      scanConfig.fetchColumn(Constants.COUNT_WAIT_COL.getFamily(), Constants.COUNT_WAIT_COL.getQualifier());

      RowIterator rowIter = snap.get(scanConfig);

      int totalWait = 0;

      int otherNodeSizes = 0;

      Stats stats = new Stats();

      while (rowIter.hasNext()) {
        Entry<Bytes,ColumnIterator> rowEntry = rowIter.next();
        String row = rowEntry.getKey().toString();
        Node node = new Node(row);

        if (node.getNodeSize() == nodeSize) {
          ColumnIterator colIter = rowEntry.getValue();
          while (colIter.hasNext()) {
            Entry<Column,Bytes> col = colIter.next();
            int wait = Integer.parseInt(col.getValue().toString());
            totalWait += wait;
            stats.incrementLevelWait(node.getLevel(), wait);
          }
        } else {
          otherNodeSizes++;
        }
      }

      String rootRow = Node.generateRootId(nodeSize);
      TypedSnapshot tsnap = Constants.TYPEL.wrap(snap);

      stats.totalWait = totalWait;
      stats.rootValue = tsnap.get().row(rootRow).col(Constants.COUNT_SEEN_COL).toInteger(0);
      stats.sawOtherNodes = otherNodeSizes != 0;

      return stats;
    }

  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      log.error("Usage: " + Print.class.getSimpleName() + "<fluo props> <node size>");
      System.exit(-1);
    }

    int nodeSize = Integer.parseInt(args[1]);

    System.out.println("Scanning wait column ....");
    Stats stats = getStats(new FluoConfiguration(new File(args[0])), nodeSize);

    for (Entry<Integer,Integer> entry : stats.levelWait.entrySet()) {
      System.out.println("Level " + entry.getKey() + " wait : " + entry.getValue() + " #nodes:" + stats.nodesPerLevel.get(entry.getKey()));
    }

    System.out.println("Total wait : " + stats.totalWait);
    System.out.println("Root value : " + stats.rootValue);
    if (stats.sawOtherNodes) {
      System.err.println("WARN : Other node sizes were seen and ignored.");
    }
  }
}
