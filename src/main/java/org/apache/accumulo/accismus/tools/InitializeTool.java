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
package org.apache.accumulo.accismus.tools;

import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.accumulo.accismus.Column;
import org.apache.accumulo.accismus.Configuration;
import org.apache.accumulo.accismus.Constants.Props;
import org.apache.accumulo.accismus.Operations;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 */
public class InitializeTool extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new InitializeTool(), args);
  }
  
  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage : " + InitializeTool.class.getSimpleName() + " <props file>  <init file>");
      System.exit(-1);
    }
    
    Properties props = new Properties(Configuration.getDefaultProperties());
    props.load(new FileReader(args[0]));
    
    Connector conn = new ZooKeeperInstance(props.getProperty(Props.ACCUMULO_INSTANCE), props.getProperty(Props.ZOOKEEPER_CONNECT))
        .getConnector(
        props.getProperty(Props.ACCUMULO_USER), new PasswordToken(props.getProperty(Props.ACCUMULO_PASSWORD)));
    
    
    Properties initProps = new Properties();
    initProps.load(new FileReader(args[1]));
    
    Map<Column,String> colObservers = new HashMap<Column,String>();
    
    Set<Entry<Object,Object>> entries = initProps.entrySet();
    for (Entry<Object,Object> entry : entries) {
      String key = (String) entry.getKey();
      if (key.startsWith("accismus.observer.")) {
        String val = (String) entry.getValue();
        String[] fields = val.split(",");
        Column col = new Column(fields[0], fields[1]).setVisibility(new ColumnVisibility(fields[2]));
        colObservers.put(col, fields[3]);
      }
    }
    
    Operations.initialize(conn, props.getProperty(Props.ZOOKEEPER_ROOT), initProps.getProperty("accismus.table"), colObservers);

    return 0;
  }
}
