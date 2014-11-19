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
package io.fluo.cluster.yarn;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.Service.State;
import io.fluo.accumulo.util.ZookeeperPath;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.FluoOracleMain;
import io.fluo.cluster.FluoWorkerMain;
import io.fluo.cluster.util.LogbackUtil;
import io.fluo.core.util.CuratorUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunResources;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.internal.RunIds;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.fluo.cluster.yarn.FluoTwillApp.FLUO_APP_NAME;

/**
 * Administers Fluo on YARN cluster
 */
public class YarnAdmin {

  private static final Logger log = LoggerFactory.getLogger(YarnAdmin.class);
  
  public static final String OBSERVER_DIR = "/observers";

  private static YarnOptions options;
  private static FluoConfiguration config;
  private static TwillRunnerService twillRunner;
  private static CuratorFramework curator;

  private static void start() throws Exception {

    TwillPreparer preparer = null;

    if (twillIdExists()) {
      String runId = getTwillId();

      TwillController controller = twillRunner.lookup(FLUO_APP_NAME, RunIds.fromString(runId));
      if ((controller != null) && controller.isRunning()) {
        System.err.println("WARNING - A YARN application " + getAppInfo()
            + " is already running for this Fluo instance!  Please stop it using 'fluo yarn stop' before starting a new one.");
        System.exit(-1);
      } else {
        logExistsButNotRunning();
        System.exit(-1);
      }
    }

    if (!config.hasRequiredOracleProps() || !config.hasRequiredWorkerProps()) {
      System.err.println("Failed to start Fluo instance because fluo.properties is missing required properties.");
      System.exit(-1);
    }
    preparer = twillRunner.prepare(new FluoTwillApp(config, options.getFluoConf()));
    
    // Add jars from fluo lib/ directory that are not being loaded by Twill. 
    // TODO Load entire directory rather than select jars.  Waiting on TWILL-108.  See FLUO-314.
    File libDir = new File(options.getFluoLib());
    for (File f : libDir.listFiles()) {
      if (f.getName().startsWith("hibernate-validator") ||
          f.getName().startsWith("javax.el-api") ||
          f.getName().startsWith("jboss-logging") ||
          f.getName().contains("graphite") ||
          f.getName().startsWith("classmate")) {
        String jarPath = "file:" + f.getCanonicalPath();
        log.trace("Adding library jar (" + f.getName() + ") to Fluo instance.");
        preparer.withResources(new URI(jarPath));
      }
    }

    // Add any observer jars found in lib/observers
    File observerDir = new File(options.getFluoLib() + OBSERVER_DIR);
    for (File f : observerDir.listFiles()) {
      String jarPath = "file:" + f.getCanonicalPath();
      log.debug("Adding observer jar (" + f.getName() + ") to Fluo instance.");
      preparer.withResources(new URI(jarPath));
    }

    Preconditions.checkNotNull(preparer, "Failed to prepare twill application");
    TwillController controller = preparer.start();
    
    log.info("Starting Fluo instance in YARN...");
    controller.start();

    // set twill run id zookeeper
    String twillId = controller.getRunId().toString();
    CuratorUtil.putData(curator, ZookeeperPath.YARN_TWILL_ID, twillId.getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
    
    while (controller.isRunning() == false) {
      Thread.sleep(500);
    }
    // set app id in zookeeper
    String appId = controller.getResourceReport().getApplicationId();
    CuratorUtil.putData(curator, ZookeeperPath.YARN_APP_ID, appId.getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
    
    log.info("Fluo instance is running in YARN " + getAppInfo());
    
    log.info("Waiting for all desired containers to start...");
    int checks = 0;
    while (allContainersRunning(controller) == false) {
      Thread.sleep(500);
      checks++;
      if (checks == 30) {
        log.warn("Still waiting... YARN may not have enough resources available for this instance.  Use ctrl-c to stop waiting and check status using 'fluo yarn info'.");
      }
    }
    log.info("Fluo instance is running all desired containers in YARN " + getAppInfo());
  }
  
  private static void stop() throws Exception {

    String twillId = verifyTwillId();

    TwillController controller = twillRunner.lookup(FLUO_APP_NAME, RunIds.fromString(twillId));
    if (controller != null) {
      System.out.print("Stopping Fluo instance " + getAppInfo()+ "...");
      controller.stopAndWait();
      System.out.println("DONE");
    } else {
      logExistsButNotRunning();
    }
    deleteZkData();
  }
  
  private static void kill() throws Exception {
    
    String twillId = verifyTwillId();

    TwillController controller = twillRunner.lookup(FLUO_APP_NAME, RunIds.fromString(twillId));
    if (controller != null) {
      System.out.print("Killing Fluo instance " + getAppInfo()+ "...");
      controller.kill();
      System.out.println("DONE");
    } else {
      logExistsButNotRunning();
    }
    deleteZkData();
  }
  
  private static boolean allContainersRunning(TwillController controller) {
    return TwillUtil.numRunning(controller, FluoOracleMain.ORACLE_NAME) == config.getOracleInstances()
        && TwillUtil.numRunning(controller, FluoWorkerMain.WORKER_NAME) == config.getWorkerInstances();
  }
  
  private static String containerStatus(TwillController controller) {
    return "" + TwillUtil.numRunning(controller, FluoOracleMain.ORACLE_NAME) + " of " + config.getOracleInstances() +
        " Oracle containers and " + TwillUtil.numRunning(controller, FluoWorkerMain.WORKER_NAME) + " of " + config.getWorkerInstances() + " Worker containers";
  }
  
  private static void status(boolean extraInfo) throws Exception {
    if (twillIdExists() == false) {
      System.out.println("A Fluo instance is not running in YARN.");
      return;
    }
    String twillId = getTwillId();
    TwillController controller = twillRunner.lookup(FLUO_APP_NAME, RunIds.fromString(twillId));
    if (controller == null) {
      logExistsButNotRunning();
      System.err.println("You can clean up this reference by running 'fluo yarn stop' or 'fluo yarn kill'.");
    } else {
      State state = controller.state();
      System.out.println("A Fluo instance is " + state + " in YARN " + getFullInfo());
      
      if (state.equals(State.RUNNING) && (allContainersRunning(controller) == false)) {
        System.out.println("\nWARNING - Fluo is not running all desired containers!  YARN may not have enough available resources.  Fluo is currently running " + containerStatus(controller));
      }

      if (extraInfo) {
        Collection<TwillRunResources> resources = controller.getResourceReport().getRunnableResources(FluoOracleMain.ORACLE_NAME);
        System.out.println("\nFluo has " + resources.size() + " of " + config.getOracleInstances() + " desired Oracle containers:\n");
        TwillUtil.printResources(resources);

        resources = controller.getResourceReport().getRunnableResources(FluoWorkerMain.WORKER_NAME);
        System.out.println("\nFluo has " + resources.size() + " of " + config.getWorkerInstances() + " desired Worker containers:\n");
        TwillUtil.printResources(resources);
      }
    }
  }
  
  private static String verifyTwillId() throws Exception {
    if (twillIdExists() == false) {
      System.err.println("WARNING - A YARN application is not referenced in Zookeeper for this Fluo instance.  Check if there is a Fluo instance "
          + "running in YARN using the command 'yarn application -list`. If so, verify that your fluo.properties is configured correctly.");
      System.exit(-1);
    }
    return getTwillId();
  }

  private static void logExistsButNotRunning() throws Exception {
    System.err.println("WARNING - A Fluo instance is not running in YARN but an instance " + getAppInfo() + " is referenced in Zookeeper");
  }
  
  private static String getAppInfo() throws Exception {
    return "(yarn id = " + getAppId() + ")";
  }
  
  private static String getFullInfo() throws Exception {
    return "(yarn id = " + getAppId() + ", twill id = "+ getTwillId() + ")";
  }
   
  private static boolean twillIdExists() throws Exception {
    return curator.checkExists().forPath(ZookeeperPath.YARN_TWILL_ID) != null;
  }

  private static String getTwillId() throws Exception {
    return new String(curator.getData().forPath(ZookeeperPath.YARN_TWILL_ID), StandardCharsets.UTF_8);
  }

  private static void deleteZkData() throws Exception {
    curator.delete().forPath(ZookeeperPath.YARN_TWILL_ID);
    curator.delete().forPath(ZookeeperPath.YARN_APP_ID);
  }
     
  private static String getAppId() throws Exception {
    return new String(curator.getData().forPath(ZookeeperPath.YARN_APP_ID), StandardCharsets.UTF_8);
  }

  public static void main(String[] args) throws ConfigurationException, Exception {

    options = new YarnOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.displayHelp()) {
      jcommand.usage();
      System.exit(-1);
    }

    LogbackUtil.init("ClusterAdmin", options.getFluoConf(), "STDOUT", false);

    File configFile = new File(options.getFluoConf() + "/fluo.properties");
    config = new FluoConfiguration(configFile);

    try {
      curator = CuratorUtil.newFluoCurator(config);
      curator.start();

      YarnConfiguration yarnConfig = new YarnConfiguration();
      yarnConfig.addResource(new Path(options.getHadoopPrefix() + "/etc/hadoop/core-site.xml"));
      yarnConfig.addResource(new Path(options.getHadoopPrefix() + "/etc/hadoop/yarn-site.xml"));

      try {
        twillRunner = new YarnTwillRunnerService(yarnConfig, config.getZookeepers() + ZookeeperPath.TWILL);
        twillRunner.startAndWait();

        // sleep to give twill time to retrieve state from zookeeper
        Thread.sleep(1000);

        switch (options.getCommand().toLowerCase()) {
          case "start":
            start();
            break;
          case "stop":
            stop();
            break;
          case "kill":
            kill();
            break;
          case "status":
            status(false);
            break;
          case "info":
            status(true);
            break;
          default:
            log.error("Unknown command: " + options.getCommand());
            break;
        }
      } finally {
        if (twillRunner != null) {
          twillRunner.stop();
        }
      }
    } finally {
      if (curator != null) {
        curator.close();
      }
    }
    
  }
}
