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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.RowColumn;
import io.fluo.core.util.AccumuloUtil;
import io.fluo.mapreduce.FluoFileOutputFormat;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mapreduce.lib.partition.RangePartitioner;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Init  extends Configured implements Tool {

  public static final String TRIE_STOP_LEVEL_PROP = FluoConfiguration.FLUO_PREFIX + ".stress.trie.stopLevel";
  public static final String TRIE_NODE_SIZE_PROP = FluoConfiguration.FLUO_PREFIX + ".stress.trie.node.size";
  
  public static class UniqueReducer extends Reducer<LongWritable,NullWritable,LongWritable,NullWritable>{
    @Override
    protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {      
      context.write(key, NullWritable.get());
    }
  }
  
  
  public static class InitMapper extends Mapper<LongWritable,NullWritable,Text,LongWritable> {

    private int stopLevel;
    private int nodeSize;
    private static final LongWritable ONE = new LongWritable(1);
    
    private Text outputKey = new Text();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      nodeSize = context.getConfiguration().getInt(TRIE_NODE_SIZE_PROP, 0);
      stopLevel = context.getConfiguration().getInt(TRIE_STOP_LEVEL_PROP, 0);
    }

    @Override
    protected void map(LongWritable key, NullWritable val, Context context) throws IOException, InterruptedException {
      Node node = new Node(key.get(), 64 / nodeSize, nodeSize);
      while (node != null) {
        outputKey.set(node.getRowId());
        context.write(outputKey, ONE);
        if (node.getLevel() <= stopLevel)
          node = null;
        else
          node = node.getParent();
      }
    }
  }
  
  public static class InitCombiner extends Reducer<Text,LongWritable,Text,LongWritable>{

    
    private LongWritable outputVal = new LongWritable();
    
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
      long sum = 0;
      for (LongWritable l : values) {
        sum += l.get();
      }
      
      outputVal.set(sum);
      context.write(key, outputVal);
    }
  }
  
  
  public static class InitReducer extends Reducer<Text,LongWritable,RowColumn,Bytes>{
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException{
      long sum = 0;
      for (LongWritable l : values) {
        sum += l.get();
      }
      
      RowColumn rowCol = new RowColumn(Bytes.of(key.getBytes(), 0, key.getLength()), Constants.COUNT_SEEN_COL);
      context.write(rowCol, Bytes.of(sum+""));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: " + this.getClass().getSimpleName() + " <fluoProps> <input dir> <tmp dir>");
      System.exit(-1);
    }

    FluoConfiguration props = new FluoConfiguration(new File(args[0]));
    Path input = new Path(args[1]);
    Path tmp = new Path(args[2]);
    
    int stopLevel;
    int nodeSize;
    try(FluoClient client = FluoFactory.newClient(props)){
      nodeSize = client.getAppConfiguration().getInt(Constants.NODE_SIZE_PROP);
      stopLevel = client.getAppConfiguration().getInt(Constants.STOP_LEVEL_PROP);
    }
    
    int ret = unique(input, new Path(tmp,"nums"));
    if(ret != 0)
      return ret;
    
    return buildTree(nodeSize, props, tmp, stopLevel);
  }

  private int unique(Path input, Path tmp) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJarByClass(Init.class);
    
    job.setJobName(Init.class.getName()+"_unique");
    
    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, input);
    
    job.setReducerClass(UniqueReducer.class);
    
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NullWritable.class);
    
    job.setOutputFormatClass(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, tmp);
    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
    
  }

  private int buildTree(int nodeSize, FluoConfiguration props, Path tmp, int stopLevel) throws Exception {
    Job job = Job.getInstance(getConf());

    job.setJarByClass(Init.class);

    job.setJobName(Init.class.getName()+"_load");
    
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    
    job.getConfiguration().setInt(TRIE_NODE_SIZE_PROP, nodeSize);
    job.getConfiguration().setInt(TRIE_STOP_LEVEL_PROP, stopLevel);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, new Path(tmp, "nums"));

    job.setMapperClass(InitMapper.class);
    job.setCombinerClass(InitCombiner.class);
    job.setReducerClass(InitReducer.class);

    job.setOutputFormatClass(FluoFileOutputFormat.class);
    job.setOutputKeyClass(RowColumn.class);
    job.setOutputValueClass(Bytes.class);
    
    job.setPartitionerClass(RangePartitioner.class);
    
    FileSystem fs = FileSystem.get(job.getConfiguration());
    Connector conn = AccumuloUtil.getConnector(props);
    
    Path splitsPath = new Path(tmp, "splits.txt");
    
    Collection<Text> splits1 = writeSplits(props, fs, conn, splitsPath);
    
    RangePartitioner.setSplitFile(job, splitsPath.toString());
    job.setNumReduceTasks(splits1.size() + 1);
    
    Path outPath = new Path(tmp, "out");
    FluoFileOutputFormat.setOutputPath(job, outPath);
    
    boolean success = job.waitForCompletion(true);
    
    if(success){
      Path failPath = new Path(tmp, "failures");
      fs.mkdirs(failPath);
      conn.tableOperations().importDirectory(props.getAccumuloTable(), outPath.toString(),failPath.toString(), false);
    }
    return success ? 0 : 1;
  }

  private Collection<Text> writeSplits(FluoConfiguration props, FileSystem fs, Connector conn, Path splitsPath) throws Exception {
    Collection<Text> splits1 = conn.tableOperations().listSplits(props.getAccumuloTable());
    OutputStream out = new BufferedOutputStream(fs.create(splitsPath));
    for (Text split : splits1) {
      out.write(Base64.encodeBase64(split.copyBytes()));
      out.write('\n');
    }

    out.close();
    return splits1;
  }
  
  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Init(), args);
    System.exit(ret);
  }
  
}
