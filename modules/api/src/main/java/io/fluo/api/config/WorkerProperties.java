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
package io.fluo.api.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Properties to configure Fluo Workers
 */
public class WorkerProperties extends ConnectionProperties implements TransactionConfiguration {

  private static final long serialVersionUID = 1L;
  public static final String WORKER_PREFIX = FLUO_PREFIX + ".worker";
  public static final String WORKER_NUM_THREADS_PROP = WORKER_PREFIX + ".numThreads";
  public static final String OBSERVER_PREFIX_PROP = WORKER_PREFIX + ".observer.";
  public static final String WORKER_INSTANCES_PROP = WORKER_PREFIX + ".instances";
  public static final String WORKER_MAX_MEMORY_PROP = WORKER_PREFIX + ".max.memory.mb";
  
  public WorkerProperties() {
    super();
    setDefaults();
  }

  public WorkerProperties(File file) throws FileNotFoundException, IOException {
    this();
    load(new FileInputStream(file));
    setDefaults();
  }

  public WorkerProperties(Properties props) {
    super(props);
    setDefaults();
  }

  private void setDefaults() {
    setDefault(WORKER_NUM_THREADS_PROP, "10");
    setDefault(ROLLBACK_TIME_PROP, ROLLBACK_TIME_DEFAULT + "");
  }

  public WorkerProperties setNumThreads(int num) {
    if (num <= 0)
      throw new IllegalArgumentException("Must be positive " + num);
    setProperty(WORKER_NUM_THREADS_PROP, num + "");
    return this;
  }

  public WorkerProperties setObservers(List<ObserverConfiguration> observers) {
    String prefix = OBSERVER_PREFIX_PROP;

    Iterator<java.util.Map.Entry<Object,Object>> iter = entrySet().iterator();
    while (iter.hasNext()) {
      String key = iter.next().getKey().toString();
      if (key.startsWith(prefix) && key.substring(prefix.length()).matches("\\d+")) {
        iter.remove();
      }
    }

    int count = 0;
    for (ObserverConfiguration oconf : observers) {

      Map<String,String> params = oconf.getParameters();
      StringBuilder paramString = new StringBuilder();
      for (java.util.Map.Entry<String,String> pentry : params.entrySet()) {
        paramString.append(',');
        paramString.append(pentry.getKey());
        paramString.append('=');
        paramString.append(pentry.getValue());
      }

      setProperty(prefix + "" + count, oconf.getClassName() + paramString);
      count++;
    }

    return this;
  }

  @Override
  public void setRollbackTime(long time, TimeUnit tu) {
    setProperty(ROLLBACK_TIME_PROP, tu.toMillis(time) + "");
  }

  public WorkerProperties setWorkerInstances(String workerInstances) {
    setProperty(WorkerProperties.WORKER_INSTANCES_PROP, workerInstances);
    return this;
  }

  public WorkerProperties setWorkerMaxMemory(String workerMaxMemory) {
    setProperty(WorkerProperties.WORKER_MAX_MEMORY_PROP, workerMaxMemory);
    return this;
  }
  
  public static Properties getDefaultProperties() {
    Properties props = new Properties();
    setDefaultProperties(props);
    return props;
  }
  
  public static void setDefaultProperties(Properties props) {
      props.put(WORKER_NUM_THREADS_PROP, "10");
      props.put(WORKER_INSTANCES_PROP, "1");
      props.put(WORKER_MAX_MEMORY_PROP, "256");
  }
}