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
package accismus.stress.trie;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accismus.api.LoaderExecutor;
import accismus.api.config.InitializationProperties;
import accismus.api.config.LoaderExecutorProperties;
import accismus.core.util.PropertyUtil;

/** MapReduce pipeline that ingests random numbers into Accismus, determines a unique set numbers,
 * and counts the number of unique numbers in that set.
 */
public class NumberIngest {
  
  private static Logger log = LoggerFactory.getLogger(NumberIngest.class);
  
  public static class IngestMapper extends MapReduceBase 
      implements Mapper<LongWritable, Text, LongWritable, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    private static LoaderExecutor le;
    private static int nodeSize;
    
    public void configure(JobConf job) {      
      InitializationProperties props = new InitializationProperties();
      props.setZookeepers(job.get("accismus.zookeeper.connect"));
      props.setZookeeperRoot(job.get("accismus.zookeeper.root"));
      props.setAccumuloInstance(job.get("accismus.accumulo.instance"));
      props.setAccumuloUser(job.get("accismus.accumulo.user"));
      props.setAccumuloPassword(job.get("accismus.accumulo.password"));
     
      nodeSize = job.getInt("trie.node.size", 4);
      
      LoaderExecutorProperties lep = new LoaderExecutorProperties(props);
      try {
        le = new LoaderExecutor(lep);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }

    @Override
    public void map(LongWritable key, Text value, 
        OutputCollector<LongWritable,IntWritable> output, Reporter reporter) throws IOException {
      int totalNum = Integer.parseInt(value.toString().trim());
      
      Random random = new Random();
      for (int i = 0; i < totalNum; i++) {
        Integer num = Math.abs(random.nextInt());
        le.execute(new NumberLoader(num, nodeSize));
        output.collect(new LongWritable(num), one);
      }
    }
  }
  
  public static class UniqueReducer extends MapReduceBase 
        implements Reducer<LongWritable, IntWritable, LongWritable, Text> {
    
    @Override
    public void reduce(LongWritable key, Iterator<IntWritable> values, 
        OutputCollector<LongWritable,Text> output, Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new Text(Integer.toString(sum)));
    }
  }
  
  public static class CountMapper extends MapReduceBase 
      implements Mapper<LongWritable, Text, Text, LongWritable> {

    private final static Text count = new Text("COUNT");
    private final static LongWritable one = new LongWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, 
        OutputCollector<Text,LongWritable> output, Reporter reporter) throws IOException {
        output.collect(count, one);
    }
  }
  
  public static class CountReducer extends MapReduceBase 
      implements Reducer<Text, LongWritable, Text, LongWritable> {
    
    private static Logger log = LoggerFactory.getLogger(UniqueReducer.class);

    @Override
    public void reduce(Text key, Iterator<LongWritable> values, 
        OutputCollector<Text,LongWritable> output, Reporter reporter) throws IOException {
      long sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      log.info("reduce "+key+" "+sum);
      output.collect(key, new LongWritable(sum));
    }
  }
    
  private static void loadConfig(JobConf conf, Properties props) {
    for (Entry<Object, Object> entry : props.entrySet()) {
      conf.set((String)entry.getKey(), (String)entry.getValue());
    }
  }
  
  private static void setupHdfs(String hadoopPrefix, String testDir,
            int numMappers, int numPerMapper) throws IllegalArgumentException, IOException {
    Configuration config = new Configuration();
    config.addResource(new Path(hadoopPrefix+"/conf/core-site.xml"));
    config.addResource(new Path(hadoopPrefix+"/conf/hdfs-site.xml"));
    FileSystem hdfs = FileSystem.get(config);
    
    String inputDir = testDir+"/input";
    
    hdfs.mkdirs(new Path(inputDir));
    FSDataOutputStream fos = hdfs.create(new Path(inputDir+"/data"));
    for (int i = 0; i < numMappers; i++) {
      fos.writeUTF(Integer.toString(numPerMapper)+"\n");
    }
    fos.close();
  }
    
  public static void main(String[] args) throws IOException, ConfigurationException {

    // Parse arguments
    if (args.length != 3) {
      log.error("Usage: NumberIngest <numMappers> <numbersPerMapper> <accismusConnectionProps>");
      System.exit(-1);
    }
    int numMappers = Integer.parseInt(args[0]);
    int numPerMapper = Integer.parseInt(args[1]);
    String connPropsPath = args[2];
    
    String hadoopPrefix = System.getenv("HADOOP_PREFIX");
    if (hadoopPrefix == null) {
      hadoopPrefix = System.getenv("HADOOP_HOME");
      if (hadoopPrefix == null) {
        log.error("HADOOP_PREFIX or HADOOP_HOME needs to be set!");
        System.exit(-1);
      }
    }
    
    // create test name
    String testId = String.format("test-%d", (new Date().getTime()/1000));
    String testDir = "/trie-stress/"+testId;
    
    setupHdfs(hadoopPrefix, testDir, numMappers, numPerMapper);
    
    JobConf ingestConf = new JobConf(NumberIngest.class);
    ingestConf.setJobName("NumberIngest");
    
    loadConfig(ingestConf, PropertyUtil.loadProps(connPropsPath));
    
    ingestConf.setOutputKeyClass(LongWritable.class);
    ingestConf.setOutputValueClass(IntWritable.class);
    ingestConf.setMapperClass(NumberIngest.IngestMapper.class);
    ingestConf.setReducerClass(NumberIngest.UniqueReducer.class);
    
    FileInputFormat.setInputPaths(ingestConf, new Path(testDir+"/input/"));
    FileOutputFormat.setOutputPath(ingestConf, new Path(testDir+"/unique/"));
    
    RunningJob ingestJob = JobClient.runJob(ingestConf);
    ingestJob.waitForCompletion();
    if (ingestJob.isSuccessful()) {
      
      JobConf countConf = new JobConf(NumberIngest.class);
      countConf.setJobName("NumberCount");
      
      countConf.setOutputKeyClass(Text.class);
      countConf.setOutputValueClass(LongWritable.class);
      countConf.setMapperClass(NumberIngest.CountMapper.class);
      countConf.setReducerClass(NumberIngest.CountReducer.class);
      
      FileInputFormat.setInputPaths(countConf, new Path(testDir+"/unique/"));
      FileOutputFormat.setOutputPath(countConf, new Path(testDir+"/output/"));
      
      RunningJob countJob = JobClient.runJob(countConf);
      countJob.waitForCompletion();
      if (countJob.isSuccessful()) {
        log.info("Ingest and count jobs were successful");
        log.info("Output can be viewed @ "+testDir);
        System.exit(0);
      } else {
        log.error("Count job failed for "+testId);
      }
    } else {
      log.error("Ingest job failed.  Skipping count job for "+testId);
    }
    
    System.exit(-1);
  }
}
