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
import java.util.TreeSet;

import io.fluo.api.config.FluoConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.hadoop.io.Text;

public class Split {

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println("Usage: " + Split.class.getSimpleName() + " <fluo props> <num tablets> <max> <node size>");
      System.exit(-1);
    }

    FluoConfiguration config = new FluoConfiguration(new File(args[0]));
    int numTablets = Integer.parseInt(args[1]);
    long max = Long.parseLong(args[2]);
    int nodeSize = Integer.parseInt(args[3]);

    int level = 64 / nodeSize;

    while (numTablets > 0) {
      TreeSet<Text> splits = genSplits(level, numTablets, max, nodeSize);
      addSplits(config, splits);

      numTablets >>= nodeSize;
      level--;
    }

  }


  private static TreeSet<Text> genSplits(int level, int numTablets, long max, int nodeSize) {

    TreeSet<Text> splits = new TreeSet<>();

    int numSplits = numTablets - 1;
    long distance = (max / numTablets) + 1;
    long split = distance;
    for (int i = 0; i < numSplits; i++) {
      Node node = new Node(split, level, nodeSize);
      splits.add(new Text(node.getRowId()));
      split += distance;
    }

    return splits;
  }

  private static void addSplits(FluoConfiguration config, TreeSet<Text> splits) throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(config.getAccumuloInstance(), config.getZookeepers());
    Connector conn = zki.getConnector(config.getAccumuloUser(), new PasswordToken(config.getAccumuloPassword()));
    conn.tableOperations().addSplits(config.getAccumuloTable(), splits);
  }

}
