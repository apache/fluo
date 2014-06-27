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
package org.fluo.api.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.fluo.api.Loader;
import org.fluo.api.LoaderExecutor;
import org.fluo.api.config.LoaderExecutorProperties;

/**
 * 
 */
public class FluoOutputFormat extends OutputFormat<Loader,NullWritable> {
  
  private static String PROPS_CONF_KEY = FluoOutputFormat.class.getName() + ".props";

  @Override
  public void checkOutputSpecs(JobContext arg0) throws IOException, InterruptedException {
    // TODO Auto-generated method stub
    
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext arg0) throws IOException, InterruptedException {
    return new OutputCommitter() {
      
      @Override
      public void setupTask(TaskAttemptContext context) throws IOException {}
      
      @Override
      public void setupJob(JobContext context) throws IOException {
        
      }
      
      @Override
      public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
        return false;
      }
      
      @Override
      public void commitTask(TaskAttemptContext context) throws IOException {}
      
      @Override
      public void abortTask(TaskAttemptContext context) throws IOException {}
    };
  }
  
  @Override
  public RecordWriter<Loader,NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
    
    ByteArrayInputStream bais = new ByteArrayInputStream(context.getConfiguration().get(PROPS_CONF_KEY).getBytes("UTF-8"));
    Properties props = new Properties();
    props.load(bais);
    
    final LoaderExecutor lexecutor;
    try {
      lexecutor = new LoaderExecutor(props);
    } catch (Exception e) {
      throw new IOException(e);
    }
    
    return new RecordWriter<Loader,NullWritable>() {
      
      @Override
      public void close(TaskAttemptContext conext) throws IOException, InterruptedException {
        lexecutor.shutdown();
      }
      
      @Override
      public void write(Loader loader, NullWritable nullw) throws IOException, InterruptedException {
        lexecutor.execute(loader);
      }
    };
  }
  
  /**
   * Call this method to initialize the Fluo connection props and {@link LoaderExecutor} props
   * 
   * @param conf
   * @param props
   *          Use {@link LoaderExecutorProperties} to set props programmatically
   */

  public static void configure(Job conf, Properties props) {
    try {
      
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      props.store(baos, "");
      
      conf.getConfiguration().set(PROPS_CONF_KEY, new String(baos.toByteArray(), "UTF8"));
      
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
