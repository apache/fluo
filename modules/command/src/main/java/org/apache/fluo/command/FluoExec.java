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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import javax.inject.Provider;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.core.client.FluoAdminImpl;

@Parameters(commandNames = "exec",
    commandDescription = "Executes <class> with <args> using classpath for <app>")
public class FluoExec extends BaseCommand implements FluoCommand {

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

  @Parameter(description = "<app> <class> args...", variableArity = true)
  private List<String> args;

  @Override
  public void execute() throws FluoCommandException {
    if (args.size() < 2) {
      throw new FluoCommandException("Usage: fluo exec <app> <class> args...");
    }
    final String applicationName = args.get(0);
    final String className = args.get(1);

    FluoConfiguration fluoConfig = CommandUtil.resolveFluoConfig();
    fluoConfig.setApplicationName(applicationName);
    CommandUtil.verifyAppInitialized(fluoConfig);
    fluoConfig = FluoAdminImpl.mergeZookeeperConfig(fluoConfig);

    try {
      Class<?> clazz = Class.forName(className);

      // inject fluo configuration
      Guice.createInjector(new FluoConfigModule(clazz, fluoConfig));

      Method method = clazz.getMethod("main", String[].class);
      List<String> execArgs = args.subList(2, args.size());
      method.invoke(null, (Object) execArgs.toArray(new String[execArgs.size()]));
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new FluoCommandException(String.format("Class %s must have a main method", className),
          e);
    } catch (ClassNotFoundException e) {
      throw new FluoCommandException(String.format("Class %s not found", className), e);
    }
  }

  public List<String> getArgs() {
    return args;
  }
}
