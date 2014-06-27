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
package org.fluo.api.config;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.fluo.api.LoaderExecutor;

/**
 * This class helps setting the properties need to create a {@link LoaderExecutor}
 */
public class LoaderExecutorProperties extends ConnectionProperties implements TransactionConfiguration {

  private static final long serialVersionUID = 1L;
  public static final String NUM_THREADS_PROP = "org.fluo.loader.executor.numThreads";
  public static final String QUEUE_SIZE_PROP = "org.fluo.loader.executor.queueSize";
  
  public LoaderExecutorProperties() {
    super();
  }

  public LoaderExecutorProperties(File file) throws FileNotFoundException, IOException {
    super(file);
    setDefaults();
  }
  
  public LoaderExecutorProperties(Properties props) {
    super(props);
    setDefaults();
  }

  public LoaderExecutorProperties setNumThreads(int numThreads) {
    setProperty(NUM_THREADS_PROP, numThreads + "");
    return this;
  }
  
  public LoaderExecutorProperties setQueueSize(int queueSize) {
    setProperty(QUEUE_SIZE_PROP, queueSize + "");
    return this;
  }

  private void setDefaults() {
    setDefault(NUM_THREADS_PROP, "10");
    setDefault(QUEUE_SIZE_PROP, "10");
  }

  @Override
  public void setRollbackTime(long time, TimeUnit tu) {
    // TODO implement this, currently only gets rollback time for worker props in zookeeper
    throw new UnsupportedOperationException();
  }

}
