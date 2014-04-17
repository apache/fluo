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

import accismus.api.Column;

/**
 * 
 */
public class WorkerProperties extends AccismusProperties {

  private static final long serialVersionUID = 1L;
  public static final String NUM_THREADS_PROP = "accismus.worker.numThreads";
  public static final String OBSERVER_PREFIX_PROP = "accismus.worker.observer.";

  public WorkerProperties() {
    super();
  }

  public WorkerProperties(File file) throws FileNotFoundException, IOException {
    this();
    load(new FileInputStream(file));
  }

  public WorkerProperties(Properties props) {
    super(props);
  }

  public WorkerProperties setNumThreads(int num) {
    if (num <= 0)
      throw new IllegalArgumentException("Must be positive " + num);
    setProperty(NUM_THREADS_PROP, num + "");
    return this;
  }

  public WorkerProperties setObservers(Map<Column,String> observers) {
    Iterator<java.util.Map.Entry<Object,Object>> iter = entrySet().iterator();
    while (iter.hasNext()) {
      if (iter.next().getKey().toString().startsWith(OBSERVER_PREFIX_PROP)) {
        iter.remove();
      }
    }

    int count = 0;
    for (java.util.Map.Entry<Column,String> entry : observers.entrySet()) {
      Column col = entry.getKey();
      setProperty(OBSERVER_PREFIX_PROP + "" + count, col.getFamily() + "," + col.getQualifier() + "," + new String(col.getVisibility().getExpression()) + ","
          + entry.getValue());
      count++;
    }

    return this;
  }
}
