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

package org.apache.fluo.core.worker;

import java.io.File;
import java.util.Objects;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.api.service.FluoWorker;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.metrics.ReporterUtil;
import org.apache.fluo.core.util.CuratorUtil;
import org.apache.fluo.core.util.UtilWaitThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FluoWorkerImpl implements FluoWorker {

  private static final Logger log = LoggerFactory.getLogger(FluoWorkerImpl.class);

  private FluoConfiguration config;
  private Environment env;
  private AutoCloseable reporters;
  private NotificationProcessor np;
  private NotificationFinder notificationFinder;
  private NodeCache appIdCache;

  public FluoWorkerImpl(FluoConfiguration config) {
    Objects.requireNonNull(config);
    Preconditions.checkArgument(config.hasRequiredWorkerProps());
    this.config = config;
  }

  @Override
  public void start() {
    try {
      env = new Environment(config);
      reporters = ReporterUtil.setupReporters(env);
      appIdCache = CuratorUtil.startAppIdWatcher(env);

      log.info("Starting Worker for Fluo '{}' application with the following configuration:",
          config.getApplicationName());
      env.getConfiguration().print();

      np = new NotificationProcessor(env);
      notificationFinder = NotificationFinderFactory.newNotificationFinder(env.getConfiguration());
      notificationFinder.init(env, np);
      notificationFinder.start();
    } catch (Exception e) {
      throw new FluoException(e);
    }
  }

  @Override
  public void stop() {
    try {
      notificationFinder.stop();
      np.close();
      appIdCache.close();
      reporters.close();
      env.close();
    } catch (Exception e) {
      throw new FluoException(e);
    }
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("Usage: FluoWorkerImpl <fluoPropsPath>");
      System.exit(-1);
    }
    String propsPath = args[0];
    Objects.requireNonNull(propsPath);
    File propsFile = new File(propsPath);
    if (!propsFile.exists()) {
      System.err.println("ERROR - Fluo properties file does not exist: " + propsPath);
      System.exit(-1);
    }
    Preconditions.checkArgument(propsFile.exists());
    try {
      FluoConfiguration config = new FluoConfiguration(propsFile);
      FluoWorkerImpl worker = new FluoWorkerImpl(config);
      worker.start();
      while (true) {
        UtilWaitThread.sleep(10000);
      }
    } catch (Exception e) {
      log.error("Exception running FluoWorker: ", e);
    }
  }
}
