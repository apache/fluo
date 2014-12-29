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
package io.fluo.mapreduce;

import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

import io.fluo.core.TestTransaction;
import io.fluo.core.TestBaseImpl;
import io.fluo.mapreduce.FluoFileOutputFormat;
import io.fluo.api.data.RowColumn;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

public class FluoFileOutputFormatIT extends TestBaseImpl {

  static final TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  public static class TestMapper extends Mapper<LongWritable,Text,RowColumn,Bytes> {

    @Override
    public void map(LongWritable key, Text data, Context context) throws IOException, InterruptedException {
      String fields[] = data.toString().split(",");

      RowColumn rc = new RowColumn(Bytes.of(fields[0]), new Column(Bytes.of(fields[1]), Bytes.of(fields[2])));
      Bytes val = Bytes.of(fields[3]);

      context.write(rc, val);
    }
  }

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder(new File(System.getProperty("user.dir") + "/target"));

  @Test
  public void testImportFile() throws Exception {


    File inDir = new File(tempFolder.getRoot(), "in");
    inDir.mkdir();
    File outDir = new File(tempFolder.getRoot(), "out");
    File failDir = new File(tempFolder.getRoot(), "fail");
    failDir.mkdir();

    // generate some data for map reduce to read
    PrintWriter writer = new PrintWriter(new File(inDir, "file1.txt"), "UTF-8");
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
    job.setOutputFormatClass(FluoFileOutputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(outDir.toURI()));
    job.setMapperClass(TestMapper.class);
    job.setNumReduceTasks(0);
    job.waitForCompletion(false);

    // bulk import rfiles
    conn.tableOperations().importDirectory(table, outDir.toString(), failDir.toString(), false);

    // read and update data using transactions
    TestTransaction tx1 = new TestTransaction(env);
    TestTransaction tx2 = new TestTransaction(env);

    Assert.assertEquals("1", tx1.get().row("a").fam("b").qual("c").toString());
    Assert.assertEquals("2", tx1.get().row("d").fam("b").qual("c").toString());
    Assert.assertEquals("90", tx1.get().row("foo").fam("moo").qual("moo").toString());

    tx1.mutate().row("a").fam("b").qual("c").set("3");
    tx1.mutate().row("d").fam("b").qual("c").delete();

    tx1.done();

    // should not see changes from tx1
    Assert.assertEquals("1", tx2.get().row("a").fam("b").qual("c").toString());
    Assert.assertEquals("2", tx2.get().row("d").fam("b").qual("c").toString());
    Assert.assertEquals("90", tx2.get().row("foo").fam("moo").qual("moo").toString());

    TestTransaction tx3 = new TestTransaction(env);

    // should see changes from tx1
    Assert.assertEquals("3", tx3.get().row("a").fam("b").qual("c").toString());
    Assert.assertNull(tx3.get().row("d").fam("b").qual("c").toString());
    Assert.assertEquals("90", tx3.get().row("foo").fam("moo").qual("moo").toString());
  }
}
