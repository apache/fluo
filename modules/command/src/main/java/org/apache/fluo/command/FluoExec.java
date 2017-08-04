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

package org.apache.fluo.command;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;

import javax.inject.Provider;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

public class FluoExec {

  private static class FluoConfigModule extends AbstractModule {

    private Class<?> clazz;
    private FluoConfiguration fluoConfig;

    FluoConfigModule(Class<?> clazz, FluoConfiguration fluoConfig) {
      this.clazz = clazz;
      this.fluoConfig = fluoConfig;
    }

    @Override
    protected void configure() {
      requestStaticInjection(clazz);
      bind(FluoConfiguration.class).toProvider((Provider<FluoConfiguration>) () -> fluoConfig);
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: FluoExec <connectionPropsPath> <applicationName> <class> args...");
      System.exit(-1);
    }
    final String connectionPropsPath = args[0];
    final String applicationName = args[1];
    final String className = args[2];
    Objects.requireNonNull(connectionPropsPath);
    File connectionPropsFile = new File(connectionPropsPath);
    Preconditions.checkArgument(connectionPropsFile.exists(), connectionPropsPath
        + " does not exist");

    FluoConfiguration fluoConfig = new FluoConfiguration(connectionPropsFile);
    fluoConfig.setApplicationName(applicationName);
    CommandUtil.verifyAppInitialized(fluoConfig);
    fluoConfig = FluoAdminImpl.mergeZookeeperConfig(fluoConfig);

    Arrays.copyOfRange(args, 3, args.length);

    Class<?> clazz = Class.forName(className);

    // inject fluo configuration
    Guice.createInjector(new FluoConfigModule(clazz, fluoConfig));

    Method method = clazz.getMethod("main", String[].class);
    method.invoke(null, (Object) Arrays.copyOfRange(args, 3, args.length));
  }
}
