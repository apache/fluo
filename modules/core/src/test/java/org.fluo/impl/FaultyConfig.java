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

import org.apache.accumulo.core.client.ConditionalWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.test.FaultyConditionalWriter;

import org.fluo.impl.Configuration;

/**
 * 
 */
public class FaultyConfig extends Configuration {
  
  private double up;
  private double wp;
  
  public FaultyConfig(Configuration config, double up, double wp) throws Exception {
    super(config);
    this.up = up;
    this.wp = wp;
  }
  
  @Override
  public Connector getConnector() {
    return super.getConnector();
  }

  public ConditionalWriter createConditionalWriter() throws TableNotFoundException {
    return new FaultyConditionalWriter(super.createConditionalWriter(), up, wp);
  }
}
