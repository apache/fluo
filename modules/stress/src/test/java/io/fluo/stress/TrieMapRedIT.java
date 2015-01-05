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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.fluo.api.config.ObserverConfiguration;
import io.fluo.core.ITBaseMini;
import io.fluo.stress.trie.Constants;
import io.fluo.stress.trie.Generate;
import io.fluo.stress.trie.Init;
import io.fluo.stress.trie.Load;
import io.fluo.stress.trie.NodeObserver;
import io.fluo.stress.trie.Print;
import io.fluo.stress.trie.Unique;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;
import org.junit.Assert;
import org.junit.Test;

/** 
 * Tests Trie Stress Test using MapReduce Ingest
 */
public class TrieMapRedIT extends ITBaseMini {
  
  @Override
  protected List<ObserverConfiguration> getObservers() {
    return Collections.singletonList(new ObserverConfiguration(NodeObserver.class.getName()));
  }

  @Override
  protected void setAppConfig(Configuration config){
    config.setProperty(Constants.STOP_LEVEL_PROP, 0);
    config.setProperty(Constants.NODE_SIZE_PROP, 8);
  }
  
  static void generate(int numMappers, int numPerMapper, int max, File out1) throws Exception {
    int ret = ToolRunner.run(new Generate(), new String[] {"-D", "mapred.job.tracker=local", "-D", "fs.defaultFS=file:///", "" + numMappers,
        numPerMapper + "", max + "", out1.toURI().toString()});
    Assert.assertEquals(0, ret);
  }

  static void load(int nodeSize, File fluoPropsFile, File input) throws Exception {
    int ret = ToolRunner
        .run(new Load(), new String[] {"-D", "mapred.job.tracker=local", "-D", "fs.defaultFS=file:///",  fluoPropsFile.getAbsolutePath(),
            input.toURI().toString()});
    Assert.assertEquals(0, ret);
  }

  static void init(int nodeSize, File fluoPropsFile, File input, File tmp) throws Exception {
    int ret = ToolRunner
        .run(new Init(), new String[] {"-D", "mapred.job.tracker=local", "-D", "fs.defaultFS=file:///", fluoPropsFile.getAbsolutePath(),
            input.toURI().toString(), tmp.toURI().toString()});
    Assert.assertEquals(0, ret);
  }

  static int unique(File... dirs) throws Exception {

    ArrayList<String> args = new ArrayList<>(Arrays.asList("-D", "mapred.job.tracker=local", "-D", "fs.defaultFS=file:///"));
    for (File dir : dirs) {
      args.add(dir.toURI().toString());
    }

    int ret = ToolRunner.run(new Unique(), args.toArray(new String[args.size()]));
    Assert.assertEquals(0, ret);
    return Unique.getNumUnique();
  }

  @Test
  public void testEndToEnd() throws Exception {
    File testDir = new File("target/MRIT");
    FileUtils.deleteQuietly(testDir);
    testDir.mkdirs();
    File fluoPropsFile = new File(testDir, "fluo.props");

    BufferedWriter propOut = new BufferedWriter(new FileWriter(fluoPropsFile));
    ConfigurationConverter.getProperties(config).store(propOut, "");
    propOut.close();

    File out1 = new File(testDir, "nums-1");

    generate(2, 100, 500, out1);
    init(8, fluoPropsFile, out1, new File(testDir, "initTmp"));
    int ucount = unique(out1);

    Assert.assertTrue(ucount > 0);

    miniFluo.waitForObservers();

    Assert.assertEquals(new Print.Stats(0, ucount, false), Print.getStats(config));

    // reload same data
    load(8, fluoPropsFile, out1);

    miniFluo.waitForObservers();

    Assert.assertEquals(new Print.Stats(0, ucount, false), Print.getStats(config));

    // load some new data
    File out2 = new File(testDir, "nums-2");
    generate(2, 100, 500, out2);
    load(8, fluoPropsFile, out2);
    int ucount2 = unique(out1, out2);
    Assert.assertTrue(ucount2 > ucount); // used > because the probability that no new numbers are chosen is exceedingly small

    miniFluo.waitForObservers();

    Assert.assertEquals(new Print.Stats(0, ucount2, false), Print.getStats(config));
    
    File out3 = new File(testDir, "nums-3");
    generate(2, 100, 500, out3);
    load(8, fluoPropsFile, out3);
    int ucount3 = unique(out1, out2, out3);
    Assert.assertTrue(ucount3 > ucount2); // used > because the probability that no new numbers are chosen is exceedingly small

    miniFluo.waitForObservers();

    Assert.assertEquals(new Print.Stats(0, ucount3, false), Print.getStats(config));
  }
}
