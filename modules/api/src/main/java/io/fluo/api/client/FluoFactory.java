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
package io.fluo.api.client;

import java.util.Properties;

import io.fluo.api.config.ConnectionProperties;
import io.fluo.api.config.InitializationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating FluoClient objects
 */
public class FluoFactory {

  private static Logger log = LoggerFactory.getLogger(FluoFactory.class);

  /**
   * Create a Fluo client 
   * 
   * @param props see {@link io.fluo.api.config.ConnectionProperties}
   * @return FluoClient
   */
  public static FluoClient newClient(Properties props) {
    String clientClassName = props.getProperty(ConnectionProperties.CLIENT_CLASS_PROP, 
                                               ConnectionProperties.DEFAULT_CLIENT_CLASS);
    try {
      return (FluoClient) Class.forName(clientClassName).getDeclaredConstructor(Properties.class).newInstance(props);
    } catch (Exception e) {
      log.error("Could not instantiate Client class - " + clientClassName);
      throw new IllegalStateException(e);
    }
  }
  
  /**
   * Create a FluoAdmin client
   * 
   * @param props see {@link io.fluo.api.config.InitializationProperties}
   * @return FluoAdmin
   */
  public static FluoAdmin newAdmin(Properties props) {
    String adminClassName = props.getProperty(InitializationProperties.ADMIN_CLASS_PROP, 
                                               InitializationProperties.DEFAULT_ADMIN_CLASS);
    try {
      return (FluoAdmin) Class.forName(adminClassName).newInstance();
    } catch (Exception e) {
      log.error("Could not instantiate Admin class - " + adminClassName);
      throw new IllegalStateException(e);
    }
  }
}
