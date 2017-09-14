/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.mapreduce.it;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import org.apache.accumulo.core.client.mapreduce.AccumuloFileOutputFormat;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.integration.ITBaseImpl;
import org.apache.fluo.integration.TestTransaction;
import org.apache.fluo.mapreduce.FluoKeyValue;
import org.apache.fluo.mapreduce.FluoKeyValueGenerator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class FluoFileOutputFormatIT extends ITBaseImpl {

  public static class TestMapper extends Mapper<LongWritable, Text, Key, Value> {

    private FluoKeyValueGenerator fkvg = new FluoKeyValueGenerator();

    @Override
    public void map(LongWritable key, Text data, Context context)
        throws IOException, InterruptedException {
      String fields[] = data.toString().split(",");

      fkvg.setRow(fields[0]).setColumn(new Column(fields[1], fields[2])).setValue(fields[3]);

      for (FluoKeyValue kv : fkvg.getKeyValues()) {
        context.write(kv.getKey(), kv.getValue());
      }
    }
  }

  @Rule
  public TemporaryFolder tempFolder =
      new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testImportFile() throws Exception {

    File inDir = new File(tempFolder.getRoot(), "in");
    inDir.mkdir();
    File outDir = new File(tempFolder.getRoot(), "out");
    File failDir = new File(tempFolder.getRoot(), "fail");
    failDir.mkdir();

    // generate some data for map reduce to read
    PrintWriter writer =
        new PrintWriter(new File(inDir, "file1.txt"), StandardCharsets.UTF_8.name());
    writer.println("a,b,c,1");
    writer.println("d,b,c,2");
    writer.println("foo,moo,moo,90");
    writer.close();

    // run map reduce job to generate rfiles
    JobConf jconf = new JobConf();
    jconf.set("mapred.job.tracker", "true");
    jconf.set("fs.defaultFS", "file:///");
    @SuppressWarnings("deprecation")
    Job job = new Job(jconf);
    job.setInputFormatClass(TextInputFormat.class);
    FileInputFormat.setInputPaths(job, inDir.toURI().toString());
    job.setOutputFormatClass(AccumuloFileOutputFormat.class);
    AccumuloFileOutputFormat.setOutputPath(job, new Path(outDir.toURI()));
    job.setMapperClass(TestMapper.class);
    job.setNumReduceTasks(0);
    Assert.assertTrue(job.waitForCompletion(false));

    // bulk import rfiles
    conn.tableOperations().importDirectory(table, outDir.toString(), failDir.toString(), false);

    // read and update data using transactions
    TestTransaction tx1 = new TestTransaction(env);
    TestTransaction tx2 = new TestTransaction(env);

    Assert.assertEquals("1", tx1.gets("a", new Column("b", "c")));
    Assert.assertEquals("2", tx1.gets("d", new Column("b", "c")));
    Assert.assertEquals("90", tx1.gets("foo", new Column("moo", "moo")));

    tx1.set("a", new Column("b", "c"), "3");
    tx1.delete("d", new Column("b", "c"));

    tx1.done();

    // should not see changes from tx1
    Assert.assertEquals("1", tx2.gets("a", new Column("b", "c")));
    Assert.assertEquals("2", tx2.gets("d", new Column("b", "c")));
    Assert.assertEquals("90", tx2.gets("foo", new Column("moo", "moo")));

    TestTransaction tx3 = new TestTransaction(env);

    // should see changes from tx1
    Assert.assertEquals("3", tx3.gets("a", new Column("b", "c")));
    Assert.assertNull(tx3.gets("d", new Column("b", "c")));
    Assert.assertEquals("90", tx3.gets("foo", new Column("moo", "moo")));
  }
}
