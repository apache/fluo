/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.api.client;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.mini.MiniFluo;
import org.apache.fluo.api.service.FluoOracle;
import org.apache.fluo.api.service.FluoWorker;

import static org.apache.fluo.api.config.FluoConfiguration.FLUO_PREFIX;

/**
 * Factory for creating {@link FluoClient}, {@link FluoAdmin}, and {@link MiniFluo}. All factory
 * methods take a configuration object which can be built using {@link FluoConfiguration}.
 *
 * @since 1.0.0
 */
public class FluoFactory {

  private static final String FLUO_IMPL_PREFIX = FLUO_PREFIX + ".impl";
  private static final String CLIENT_CLASS_PROP = FLUO_IMPL_PREFIX + ".client.class";
  private static final String CLIENT_CLASS_DEFAULT = "org.apache.fluo.core.client.FluoClientImpl";
  private static final String ADMIN_CLASS_PROP = FLUO_IMPL_PREFIX + ".admin.class";
  private static final String ADMIN_CLASS_DEFAULT = "org.apache.fluo.core.client.FluoAdminImpl";
  private static final String WORKER_CLASS_PROP = FLUO_IMPL_PREFIX + ".worker.class";
  private static final String WORKER_CLASS_DEFAULT = "org.apache.fluo.core.worker.FluoWorkerImpl";
  private static final String ORACLE_CLASS_PROP = FLUO_IMPL_PREFIX + ".oracle.class";
  private static final String ORACLE_CLASS_DEFAULT = "org.apache.fluo.core.oracle.FluoOracleImpl";
  private static final String MINI_CLASS_PROP = FLUO_IMPL_PREFIX + ".mini.class";
  private static final String MINI_CLASS_DEFAULT = "org.apache.fluo.mini.MiniFluoImpl";

  /**
   * Creates a {@link FluoClient} for reading and writing data to Fluo. {@link FluoClient#close()}
   * should be called when you are finished using it. Configuration (see {@link FluoConfiguration})
   * should contain properties with connection.* prefix. Please review all connection.* properties
   * but many have a default. At a minimum, configuration should contain the following properties
   * that have no default: fluo.connection.application.name
   */
  public static FluoClient newClient(SimpleConfiguration configuration) {
    return getAndBuildClassWithConfig(configuration, CLIENT_CLASS_PROP, CLIENT_CLASS_DEFAULT);
  }

  /**
   * Creates a {@link FluoAdmin} client for administering Fluo. Configuration (see
   * {@link FluoConfiguration}) should contain all Fluo configuration properties. Review all
   * properties but many have a default. At a minimum, configuration should contain the following
   * properties that have no default: fluo.connection.application.name, fluo.accumulo.user,
   * fluo.accumulo.password, fluo.accumulo.instance, fluo.accumulo.table, fluo.accumulo.classpath
   */
  public static FluoAdmin newAdmin(SimpleConfiguration configuration) {
    return getAndBuildClassWithConfig(configuration, ADMIN_CLASS_PROP, ADMIN_CLASS_DEFAULT);
  }

  /**
   * Creates a {@link MiniFluo} using the provided configuration. Configuration (see
   * {@link FluoConfiguration}) should either contain the property fluo.mini.start.accumulo (set to
   * true) to indicate that MiniFluo should start its own Accumulo instance or it should contain the
   * following properties if it is connecting to an existing instance: fluo.client.accumulo.user,
   * fluo.client.accumulo.password, fluo.client.accumulo.instance, fluo.admin.accumulo.table
   */
  public static MiniFluo newMiniFluo(SimpleConfiguration configuration) {
    return getAndBuildClassWithConfig(configuration, MINI_CLASS_PROP, MINI_CLASS_DEFAULT);
  }

  /**
   * Creates a {@link FluoOracle}. Configuration (see {@link FluoConfiguration}) should contain
   * properties with connection.* prefix. Please review all connection.* properties but many have a
   * default. At a minimum, configuration should contain the following properties that have no
   * default: fluo.connection.application.name
   */
  public static FluoOracle newOracle(SimpleConfiguration configuration) {
    return getAndBuildClassWithConfig(configuration, ORACLE_CLASS_PROP, ORACLE_CLASS_DEFAULT);
  }

  /**
   * Creates a {@link FluoWorker}. Configuration (see {@link FluoConfiguration}) should contain
   * properties with connection.* prefix. Please review all connection.* properties but many have a
   * default. At a minimum, configuration should contain the following properties that have no
   * default: fluo.connection.application.name
   */
  public static FluoWorker newWorker(SimpleConfiguration configuration) {
    return getAndBuildClassWithConfig(configuration, WORKER_CLASS_PROP, WORKER_CLASS_DEFAULT);
  }

  private static <T> T getAndBuildClassWithConfig(SimpleConfiguration configuration,
      String classProp, String classDefault) {
    FluoConfiguration config = new FluoConfiguration(configuration);
    String clazz = config.getString(classProp, classDefault);
    Objects.requireNonNull(clazz, classProp + " cannot be null");
    Preconditions.checkArgument(!clazz.isEmpty(), classProp + " cannot be empty");
    return buildClassWithConfig(clazz, config);
  }

  @SuppressWarnings("unchecked")
  private static <T> T buildClassWithConfig(String clazz, FluoConfiguration config) {
    try {
      return (T) Class.forName(clazz).getDeclaredConstructor(FluoConfiguration.class)
          .newInstance(config);
    } catch (ClassNotFoundException e) {
      String msg =
          "Could not find " + clazz
              + " class which could be caused by fluo-core jar not being on the classpath.";
      throw new FluoException(msg, e);
    } catch (InvocationTargetException e) {
      String msg = "Failed to construct " + clazz + " class due to exception";
      throw new FluoException(msg, e);
    } catch (Exception e) {
      throw new FluoException(e);
    }
  }
}
