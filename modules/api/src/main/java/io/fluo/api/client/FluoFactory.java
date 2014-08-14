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
import io.fluo.api.config.MiniFluoProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Fluo client objects
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

    return buildClassWithProps(props, clientClassName);
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

    return buildClass(adminClassName);
  }

  /**
   * Create a MiniFluo instance
   *
   * @param props see {@link io.fluo.api.config.MiniFluoProperties}
   * @return MiniFluo
   */
  public static MiniFluo newMiniFluo(Properties props) {

    String miniFluoClassName = props.getProperty(MiniFluoProperties.MINI_CLASS_PROP,
                                                 MiniFluoProperties.DEFAULT_MINI_CLASS);

    return buildClassWithProps(props, miniFluoClassName);
  }

  private static <T>T buildClassWithProps(Properties props, String clazz) {
    try {
      return (T) Class.forName(clazz).getDeclaredConstructor(Properties.class).newInstance(props);
    } catch (Exception e) {
      log.error("Could not instantiate class - " + clazz);
      throw new IllegalStateException(e);
    }
  }

  private static <T>T buildClass(String clazz) {
    try {
      return (T) Class.forName(clazz).newInstance();
    } catch (Exception e) {
      log.error("Could not instantiate class - " + clazz);
      throw new IllegalStateException(e);
    }
  }

}
