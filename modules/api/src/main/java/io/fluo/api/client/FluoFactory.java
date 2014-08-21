/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fluo.api.client;

import io.fluo.api.config.FluoConfiguration;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating Fluo client objects
 */
public class FluoFactory {

  private static Logger log = LoggerFactory.getLogger(FluoFactory.class);

  /**
   * Creates a Fluo client.  Configuration should contain properties 
   * with client.* prefix.  Please review all client.* properties 
   * but many have a default.  At a minimum, configuration should
   * contain the following properties that have no default:
   * 
   * io.fluo.client.accumulo.user
   * io.fluo.client.accumulo.password
   * io.fluo.client.accumulo.instance
   * 
   * @param configuration see {@link io.fluo.api.config.FluoConfiguration}
   * @return FluoClient
   */
  public static FluoClient newClient(Configuration configuration) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    return buildClassWithConfig(config.getClientClass(), config);
  }
  
  /**
   * Creates a FluoAdmin client.  Configuration should contain properties 
   * with client.* and admin.* prefix.  Please review all properties 
   * but many have a default.  At a minimum, configuration should
   * contain the following properties that have no default:
   * 
   * io.fluo.client.accumulo.user
   * io.fluo.client.accumulo.password
   * io.fluo.client.accumulo.instance
   * io.fluo.admin.accumulo.table
   * 
   * @param configuration see {@link io.fluo.api.config.FluoConfiguration}
   * @return FluoAdmin
   */
  public static FluoAdmin newAdmin(Configuration configuration) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    return buildClassWithConfig(config.getAdminClass(), config);
  }

  /**
   * Creates a MiniFluo instance.  Please review all properties in 
   * fluo.properties.  At a minimum, configuration should contain 
   * the following properties that have no default:
   * 
   * io.fluo.client.accumulo.user
   * io.fluo.client.accumulo.password
   * io.fluo.client.accumulo.instance
   * io.fluo.admin.accumulo.table
   *
   * @param configuration see {@link io.fluo.api.config.FluoConfiguration}
   * @return MiniFluo
   */
  public static MiniFluo newMiniFluo(Configuration configuration) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    return buildClassWithConfig(config.getMiniClass(), config);
  }

  @SuppressWarnings("unchecked")
  private static <T>T buildClassWithConfig(String clazz, Configuration config) {
    try {
      return (T) Class.forName(clazz).getDeclaredConstructor(FluoConfiguration.class).newInstance(config);
    } catch (Exception e) {
      log.error("Could not instantiate class - " + clazz);
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings({"unchecked", "unused"})
  private static <T>T buildClass(String clazz) {
    try {
      return (T) Class.forName(clazz).newInstance();
    } catch (Exception e) {
      log.error("Could not instantiate class - " + clazz);
      throw new IllegalStateException(e);
    }
  }
}
