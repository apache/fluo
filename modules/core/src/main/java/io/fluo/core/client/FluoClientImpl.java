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
package io.fluo.core.client;

import java.util.Properties;

import io.fluo.api.client.FluoClient;
import io.fluo.api.client.LoaderExecutor;
import io.fluo.api.client.Snapshot;
import io.fluo.core.impl.Environment;
import io.fluo.core.impl.TransactionImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Fluo Client
 */
public class FluoClientImpl implements FluoClient {
  
  private static Logger log = LoggerFactory.getLogger(FluoClientImpl.class);
  private Properties props;
  private Environment env;
  
  public FluoClientImpl(Properties properties) {
    this.props = properties;
    try {
      this.env = new Environment(props);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public LoaderExecutor newLoaderExecutor() {
    try {
      return new LoaderExecutorImpl(props);
    } catch (Exception e) {
      log.error("Failed to create a LoaderExecutor");
      throw new IllegalStateException(e);
    }
  }

  @Override
  public Snapshot newSnapshot() {
    try {
      return new TransactionImpl(env);
    } catch (Exception e) {
      log.error("Failed to create a Snapshot");
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() {
    env.close();
  }
}
