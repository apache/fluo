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
package org.fluo.impl;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

/**
 * 
 */
public class Constants {
  public static final ByteSequence NOTIFY_CF = new ArrayByteSequence("ntfy");
  
  public static class Zookeeper {
    public static final String CONFIG = "/config";
    public static final String TABLE = CONFIG + "/accumulo.table";
    public static final String ACCUMULO_INSTANCE_NAME = CONFIG + "/accumulo.instance.name";
    public static final String ACCUMULO_INSTANCE_ID = CONFIG + "/accumulo.instance.id";
    public static final String FLUO_INSTANCE_ID = CONFIG + "/org.fluo.instance.id";
    public static final String OBSERVERS = CONFIG + "/org.fluo.observers";
    public static final String WORKER_CONFIG = CONFIG + "/org.fluo.workers";
    
    public static final String ORACLE = "/oracle";
    public static final String TIMESTAMP = ORACLE + "/timestamp";
    public static final String ORACLE_SERVER = ORACLE + "/server";
  }

  public static final String WORKER_THREADS = "org.fluo.config.worker.numThreads";

  public static final long ROLLBACK_TIME_DEFAULT = 300000;
}
