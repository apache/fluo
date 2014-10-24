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

import java.io.IOException;
import java.util.Iterator;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Unique extends Configured implements Tool {

  private static final Logger log = LoggerFactory.getLogger(Unique.class);

  public static enum Stats {
    UNIQUE;
  }
  
  public static class UniqueReducer extends MapReduceBase implements Reducer<LongWritable,NullWritable,LongWritable,NullWritable> {
    @Override
    public void reduce(LongWritable key, Iterator<NullWritable> values, OutputCollector<LongWritable,NullWritable> output, Reporter reporter) throws IOException {
      reporter.getCounter(Stats.UNIQUE).increment(1);
    }
  }


  private static int numUnique = 0;

  @VisibleForTesting
  public static int getNumUnique() {
    return numUnique;
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length < 1) {
      log.error("Usage: " + this.getClass().getSimpleName() + "<input dir>{ <input dir>}");
      System.exit(-1);
    }

    JobConf job = new JobConf(getConf());

    job.setJarByClass(Unique.class);

    job.setInputFormat(SequenceFileInputFormat.class);
    for (String arg : args) {
      SequenceFileInputFormat.addInputPath(job, new Path(arg));
    }

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setReducerClass(UniqueReducer.class);
    
    job.setOutputFormat(NullOutputFormat.class);
    
    RunningJob runningJob = JobClient.runJob(job);
    runningJob.waitForCompletion();
    numUnique = (int) runningJob.getCounters().getCounter(Stats.UNIQUE);
    
    log.debug("numUnique : "+numUnique);
    
    return runningJob.isSuccessful() ? 0 : -1;
    
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Unique(), args);
    System.exit(ret);
  }

}
