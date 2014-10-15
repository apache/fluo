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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Random;

import com.google.common.base.Preconditions;
import io.fluo.api.config.FluoConfiguration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Generate extends Configured implements Tool {

  private static final Logger log = LoggerFactory.getLogger(Generate.class);

  public static final String TRIE_GEN_NUM_PER_MAPPER_PROP = FluoConfiguration.FLUO_PREFIX + ".stress.trie.numPerMapper";
  public static final String TRIE_GEN_NUM_MAPPERS_PROP = FluoConfiguration.FLUO_PREFIX + ".stress.trie.numMappers";
  public static final String TRIE_GEN_MAX_PROP = FluoConfiguration.FLUO_PREFIX + ".stress.trie.max";

  public static class RandomSplit implements InputSplit {

    @Override
    public void write(DataOutput out) throws IOException {}

    @Override
    public void readFields(DataInput in) throws IOException {}

    @Override
    public long getLength() throws IOException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
      return new String[0];
    }

  }

  public static class RandomLongInputFormat implements InputFormat<LongWritable,NullWritable> {

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
      InputSplit[] splits = new InputSplit[job.getInt(TRIE_GEN_NUM_MAPPERS_PROP, 1)];
      for (int i = 0; i < splits.length; i++) {
        splits[i] = new RandomSplit();
      }
      return splits;
    }

    @Override
    public RecordReader<LongWritable,NullWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {

      final int numToGen = job.getInt(TRIE_GEN_NUM_PER_MAPPER_PROP, 1);
      final long max = job.getLong(TRIE_GEN_MAX_PROP, Long.MAX_VALUE);

      return new RecordReader<LongWritable,NullWritable>() {

        private Random random = new Random();
        private int count = 0;

        @Override
        public boolean next(LongWritable key, NullWritable value) throws IOException {

          if (count == numToGen)
            return false;

          key.set((random.nextLong() & 0x7fffffffffffffffl) % max);
          count++;
          return true;
        }

        @Override
        public LongWritable createKey() {
          return new LongWritable();
        }

        @Override
        public NullWritable createValue() {
          return NullWritable.get();
        }

        @Override
        public long getPos() throws IOException {
          return count;
        }

        @Override
        public void close() throws IOException {}

        @Override
        public float getProgress() throws IOException {
          return (float) count / numToGen;
        }
      };
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 4) {
      log.error("Usage: " + this.getClass().getSimpleName() + " <numMappers> <numbersPerMapper> <max> <output dir>");
      System.exit(-1);
    }

    int numMappers = Integer.parseInt(args[0]);
    int numPerMapper = Integer.parseInt(args[1]);
    long max = Long.parseLong(args[2]);
    Path out = new Path(args[3]);

    Preconditions.checkArgument(numMappers > 0, "numMappers <= 0");
    Preconditions.checkArgument(numPerMapper > 0, "numPerMapper <= 0");
    Preconditions.checkArgument(max > 0, "max <= 0");

    JobConf job = new JobConf(getConf());

    job.setJobName(this.getClass().getName());

    job.setJarByClass(Generate.class);

    job.setInt(TRIE_GEN_NUM_PER_MAPPER_PROP, numPerMapper);
    job.setInt(TRIE_GEN_NUM_MAPPERS_PROP, numMappers);
    job.setLong(TRIE_GEN_MAX_PROP, max);

    job.setInputFormat(RandomLongInputFormat.class);

    job.setNumReduceTasks(0);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(NullWritable.class);

    job.setOutputFormat(SequenceFileOutputFormat.class);
    SequenceFileOutputFormat.setOutputPath(job, out);

    RunningJob runningJob = JobClient.runJob(job);
    runningJob.waitForCompletion();
    return runningJob.isSuccessful() ? 0 : -1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Generate(), args);
    System.exit(ret);
  }

}
