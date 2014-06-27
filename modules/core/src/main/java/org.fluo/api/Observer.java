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
package org.fluo.api;

import java.util.Map;

import org.apache.accumulo.core.data.ByteSequence;

/**
 * An observer is created for each worker thread and re-used for the lifetime of a worker thread.
 * 
 * Consider extending {@link AbstractObserver} instead of implementing this. The abstract class will shield you from the addition of interface methods.
 */
public interface Observer {
  public void init(Map<String,String> config) throws Exception;
  public void process(Transaction tx, ByteSequence row, Column col) throws Exception;
  public void close();
}
