/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.fluo.api.client;

import java.lang.reflect.InvocationTargetException;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.exceptions.FluoException;
import io.fluo.api.mini.MiniFluo;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating {@link FluoClient}, {@link FluoAdmin}, and {@link MiniFluo}. All factory
 * methods take a configuration object which can be built using {@link FluoConfiguration}.
 */
public class FluoFactory {

  private static final Logger log = LoggerFactory.getLogger(FluoFactory.class);

  /**
   * Creates a {@link FluoClient} for reading and writing data to Fluo. {@link FluoClient#close()}
   * should be called when you are finished using it. Configuration (see {@link FluoConfiguration})
   * should contain properties with client.* prefix. Please review all client.* properties but many
   * have a default. At a minimum, configuration should contain the following properties that have
   * no default: io.fluo.client.accumulo.user, io.fluo.client.accumulo.password,
   * io.fluo.client.accumulo.instance
   */
  public static FluoClient newClient(Configuration configuration) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    return buildClassWithConfig(config.getClientClass(), config);
  }

  /**
   * Creates a {@link FluoAdmin} client for administering Fluo. Configuration (see
   * {@link FluoConfiguration}) should contain properties with client.* and admin.* prefix. Please
   * review all properties but many have a default. At a minimum, configuration should contain the
   * following properties that have no default: io.fluo.client.accumulo.user,
   * io.fluo.client.accumulo.password, io.fluo.client.accumulo.instance,
   * io.fluo.admin.accumulo.table, io.fluo.admin.accumulo.classpath
   */
  public static FluoAdmin newAdmin(Configuration configuration) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    return buildClassWithConfig(config.getAdminClass(), config);
  }

  /**
   * Creates a {@link MiniFluo} using the provided configuration.
   * 
   * Configuration (see {@link FluoConfiguration}) should either contain the property
   * io.fluo.mini.start.accumulo (set to true) to indicate that MiniFluo should start its own
   * Accumulo instance or it should contain the following properties if it is connecting to an
   * existing instance: io.fluo.client.accumulo.user, io.fluo.client.accumulo.password,
   * io.fluo.client.accumulo.instance, io.fluo.admin.accumulo.table
   */
  public static MiniFluo newMiniFluo(Configuration configuration) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    return buildClassWithConfig(config.getMiniClass(), config);
  }

  @SuppressWarnings("unchecked")
  private static <T> T buildClassWithConfig(String clazz, Configuration config) {
    try {
      return (T) Class.forName(clazz).getDeclaredConstructor(FluoConfiguration.class)
          .newInstance(config);
    } catch (ClassNotFoundException e) {
      String msg =
          "Could not find " + clazz
              + " class which could be caused by fluo-core jar not being on the classpath.";
      log.error(msg);
      throw new FluoException(msg, e);
    } catch (InvocationTargetException e) {
      String msg = "Failed to construct " + clazz + " class due to exception";
      log.error(msg, e);
      throw new FluoException(msg, e);
    } catch (Exception e) {
      log.error("Could not instantiate class - " + clazz);
      throw new FluoException(e);
    }
  }

  @SuppressWarnings({"unchecked", "unused"})
  private static <T> T buildClass(String clazz) {
    try {
      return (T) Class.forName(clazz).newInstance();
    } catch (ClassNotFoundException e) {
      String msg =
          "Could not find " + clazz
              + " class which could be caused by fluo-core jar not being on the classpath.";
      log.error(msg);
      throw new FluoException(msg, e);
    } catch (Exception e) {
      log.error("Could not instantiate class - " + clazz);
      throw new FluoException(e);
    }
  }
}
