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
import java.io.IOException;

import io.fluo.api.client.Loader;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.mapreduce.FluoOutputFormat;
import org.apache.commons.configuration.ConfigurationConverter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Load extends Configured implements Tool {

  private static final Logger log = LoggerFactory.getLogger(Load.class);

  public static final String TRIE_NODE_SIZE_PROP = FluoConfiguration.FLUO_PREFIX + ".stress.trie.node.size";

  public static class LoadMapper extends Mapper<LongWritable,NullWritable,Loader,NullWritable> {

    private int nodeSize;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
      nodeSize = context.getConfiguration().getInt(TRIE_NODE_SIZE_PROP, 0);
    }

    @Override
    protected void map(LongWritable key, NullWritable val, Context context) throws IOException, InterruptedException {
      context.write(new NumberLoader(key.get(), nodeSize), val);
    }
  }

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      log.error("Usage: " + this.getClass().getSimpleName() + "<nodeSize> <fluoProps> <input dir>");
      System.exit(-1);
    }

    int nodeSize = Integer.parseInt(args[0]);
    FluoConfiguration props = new FluoConfiguration(new File(args[1]));
    Path input = new Path(args[2]);

    Job job = Job.getInstance(getConf());

    job.setJarByClass(Load.class);

    job.getConfiguration().setInt(TRIE_NODE_SIZE_PROP, nodeSize);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    SequenceFileInputFormat.addInputPath(job, input);

    job.setMapperClass(LoadMapper.class);

    job.setNumReduceTasks(0);

    job.setOutputFormatClass(FluoOutputFormat.class);
    FluoOutputFormat.configure(job, ConfigurationConverter.getProperties(props));

    job.getConfiguration().setBoolean("mapreduce.map.speculative", false);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int ret = ToolRunner.run(new Load(), args);
    System.exit(ret);
  }

}
