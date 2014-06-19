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
package accismus.api.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import accismus.api.Column;
import accismus.impl.Constants;

/**
 * 
 */
public class WorkerProperties extends AccismusProperties implements TransactionConfiguration {

  private static final long serialVersionUID = 1L;
  public static final String NUM_THREADS_PROP = "accismus.worker.numThreads";
  public static final String OBSERVER_PREFIX_PROP = "accismus.worker.observer.";
  public static final String WEAK_OBSERVER_PREFIX_PROP = "accismus.worker.observer.weak.";
  public static final String WORKER_INSTANCES_PROP = "accismus.worker.instances";
  public static final String WORKER_MAX_MEMORY_PROP = "accismus.worker.max.memory.mb";

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
    setDefault(NUM_THREADS_PROP, "10");
    setDefault(TransactionConfiguration.ROLLBACK_TIME_PROP, Constants.ROLLBACK_TIME_DEFAULT + "");
  }

  public WorkerProperties setNumThreads(int num) {
    if (num <= 0)
      throw new IllegalArgumentException("Must be positive " + num);
    setProperty(NUM_THREADS_PROP, num + "");
    return this;
  }

  private WorkerProperties setObservers(Map<Column,ObserverConfiguration> observers, String prefix) {
    Iterator<java.util.Map.Entry<Object,Object>> iter = entrySet().iterator();
    while (iter.hasNext()) {
      String key = iter.next().getKey().toString();
      if (key.startsWith(prefix) && key.substring(prefix.length()).matches("\\d+")) {
        iter.remove();
      }
    }

    int count = 0;
    for (java.util.Map.Entry<Column,ObserverConfiguration> entry : observers.entrySet()) {
      Column col = entry.getKey();

      Map<String,String> params = entry.getValue().getParameters();
      StringBuilder paramString = new StringBuilder();
      for (java.util.Map.Entry<String,String> pentry : params.entrySet()) {
        paramString.append(',');
        paramString.append(pentry.getKey());
        paramString.append('=');
        paramString.append(pentry.getValue());
      }

      setProperty(prefix + "" + count, col.getFamily() + "," + col.getQualifier() + "," + new String(col.getVisibility().getExpression()) + ","
          + entry.getValue().getClassName() + paramString);
      count++;
    }

    return this;
  }

  public WorkerProperties setWeakObservers(Map<Column,ObserverConfiguration> observers) {
    return setObservers(observers, WEAK_OBSERVER_PREFIX_PROP);
  }

  public WorkerProperties setObservers(Map<Column,ObserverConfiguration> observers) {
    return setObservers(observers, OBSERVER_PREFIX_PROP);
  }

  @Override
  public void setRollbackTime(long time, TimeUnit tu) {
    setProperty(TransactionConfiguration.ROLLBACK_TIME_PROP, tu.toMillis(time) + "");
  }
  
  public WorkerProperties setWorkerInstances(String workerInstances) {
    setProperty(WorkerProperties.WORKER_INSTANCES_PROP, workerInstances);
    return this;
  }
  
  public WorkerProperties setWorkerMaxMemory(String workerMaxMemory) {
    setProperty(WorkerProperties.WORKER_MAX_MEMORY_PROP, workerMaxMemory);
    return this;
  }
}
