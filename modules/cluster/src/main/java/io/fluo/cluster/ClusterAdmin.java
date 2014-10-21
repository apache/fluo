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
package io.fluo.cluster;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import com.beust.jcommander.JCommander;
import com.google.common.base.Preconditions;
import io.fluo.accumulo.util.ZookeeperConstants;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.util.ClusterUtil;
import io.fluo.cluster.util.Logging;
import io.fluo.core.util.CuratorUtil;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.internal.RunIds;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterAdmin {

  private static final Logger log = LoggerFactory.getLogger(ClusterAdmin.class);

  private static ClusterAdminOptions options;
  private static FluoConfiguration config;
  private static TwillRunnerService twillRunner;
  private static CuratorFramework curator;

  private static void start() throws Exception {

    TwillPreparer preparer = null;

    if (runIdExists()) {
      String runId = getRunId();
      
      TwillController controller = twillRunner.lookup(app(), RunIds.fromString(runId));
      if ((controller != null) && controller.isRunning()) {
        System.out.println("WARNING - A " + app() + " application is already running for this Fluo instance!  Please stop it before starting a new one.");
        System.exit(-1);
      } else {
        log.warn(getAppId(runId) + " is not running but is referenced in Zookeeper for this Fluo instance.  Removing reference and starting a new one.");
        deleteRunId();
      }
    }

    if (app().equalsIgnoreCase(ClusterUtil.ORACLE_APP_NAME)) {
      if (!config.hasRequiredOracleProps()) {
        log.error("fluo.properties is missing required properties for oracle");
        System.exit(-1);
      }
      preparer = twillRunner.prepare(new OracleApp(config, options.getFluoHome()));
    } else if (app().equalsIgnoreCase(ClusterUtil.WORKER_APP_NAME)) {
      if (!config.hasRequiredWorkerProps()) {
        log.error("fluo.properties is missing required properties for worker");
        System.exit(-1);
      }
      preparer = twillRunner.prepare(new WorkerApp(config, options.getFluoHome()));

      // Add any observer jars found in lib observers
      File observerDir = new File(options.getFluoHome() + "/lib/observers");
      for (File f : observerDir.listFiles()) {
        String jarPath = "file:" + f.getCanonicalPath();
        log.debug("Adding observer jar (" + f.getName() + ") to FluoWorker app");
        preparer.withResources(new URI(jarPath));
      }
    } else {
      log.error("Attempted to start unknown Fluo application - " + options.getApplication());
      System.exit(-1);
    }

    Preconditions.checkNotNull(preparer, "Failed to prepare twill application");
    TwillController controller = preparer.start();
    
    log.info("Starting " + app() + " application...");
    controller.start();
    
    String runId = controller.getRunId().toString();
    setRunId(runId);
    
    while (controller.isRunning() == false) {
      Thread.sleep(500);
    }
    log.info("Started "+ getAppId(runId));
  }
  
  private static String getAppId(String runId) {
    return app()+ " (twill id = " + runId+ ")";
  }
  
  private static void stopOrKill(String cmd) throws Exception {
    if (runIdExists() == false) {
      System.out.println("WARNING - No " + app() + " referenced in Zookeeper for this Fluo instance.  Check if there is a " + app()
          + " running in YARN using the command 'yarn application -list`. If so, verify that your fluo.properties is configured for the correct Fluo instance.");
      return;
    }

    String runId = getRunId();

    TwillController controller = twillRunner.lookup(app(), RunIds.fromString(runId));
    if ((controller != null) && controller.isRunning()) {
      if (cmd.equals("stop")) {
        System.out.print("Stopping " + getAppId(runId) + "...");
        controller.stopAndWait();
      } else if (cmd.equals("kill")) {
        System.out.print("Killing " + getAppId(runId) + "...");
        controller.kill();
      } else {
        log.error("Unknown command - " + cmd);
        System.exit(-1);
      }
      System.out.println("DONE");
    } else {
      System.out.println("WARNING - " + getAppId(runId) + " is not running but is referenced in Zookeeper for this Fluo instance.");
    }
    
    deleteRunId();
  }
    
  private static void status() throws Exception {
    if (runIdExists() == false) {
      System.out.println("WARNING - No " + app() + " referenced in Zookeeper for this Fluo instance.  Check if there is a " + app()
          + " running in YARN using the command 'yarn application -list`. If so, verify that your fluo.properties is configured for the correct Fluo instance.");
      return;
    }
    String runId = getRunId();

    TwillController controller = twillRunner.lookup(app(), RunIds.fromString(runId));
    if ((controller != null) && controller.isRunning()) {
      System.out.println(getAppId(runId) + " is running.");
    } else {
      System.out.println("WARNING - " + getAppId(runId) + " is not running but is referenced in Zookeeper for this Fluo instance.");
    }
  }

  private static String getAppRunIdPath() {
    if (app().equalsIgnoreCase(ClusterUtil.WORKER_APP_NAME)) {
      return ZookeeperConstants.twillWorkerIdPath(config.getZookeeperRoot());
    } else if (app().equalsIgnoreCase(ClusterUtil.ORACLE_APP_NAME)) {
      return ZookeeperConstants.twillOracleIdPath(config.getZookeeperRoot());
    } else {
      log.error("Unknown Fluo application - " + app());
      System.exit(-1);
    }
    return null;
  }
  
  private static String app() {
    return options.getApplication().trim();
  }
 
  private static boolean runIdExists() throws Exception {
    return curator.checkExists().forPath(getAppRunIdPath()) != null;
  }

  private static String getRunId() throws Exception {
    return new String(curator.getData().forPath(getAppRunIdPath()), StandardCharsets.UTF_8);
  }

  private static void setRunId(String runId) throws Exception {
    CuratorUtil.putData(curator, getAppRunIdPath(), runId.getBytes(StandardCharsets.UTF_8), CuratorUtil.NodeExistsPolicy.FAIL);
  }

  private static void deleteRunId() throws Exception {
    curator.delete().forPath(getAppRunIdPath());
  }

  public static void main(String[] args) throws ConfigurationException, Exception {

    options = new ClusterAdminOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.displayHelp()) {
      jcommand.usage();
      System.exit(-1);
    }
    
    Preconditions.checkArgument(options.getApplication().equalsIgnoreCase(ClusterUtil.WORKER_APP_NAME) ||
        options.getApplication().equalsIgnoreCase(ClusterUtil.ORACLE_APP_NAME), "Unknown Fluo application");
      
    Logging.init("ClusterAdmin", options.getFluoHome() + "/conf", "STDOUT", false);

    File configFile = new File(options.getFluoHome() + "/conf/fluo.properties");
    config = new FluoConfiguration(configFile);

    curator = CuratorUtil.getCurator(config);
    curator.start();

    YarnConfiguration yarnConfig = new YarnConfiguration();
    yarnConfig.addResource(new Path(options.getHadoopPrefix() + "/etc/hadoop/core-site.xml"));
    yarnConfig.addResource(new Path(options.getHadoopPrefix() + "/etc/hadoop/yarn-site.xml"));

    twillRunner = new YarnTwillRunnerService(yarnConfig, config.getZookeepers());
    twillRunner.startAndWait();
      
    // sleep to give twill time to retrieve state from zookeeper
    Thread.sleep(1000);
    
    switch (options.getCommand().toLowerCase()) {
      case "start":
        start();
        break;
      case "stop":
        stopOrKill("stop");
        break;
      case "kill":
        stopOrKill("kill");
        break;
      case "status":
        status();
        break;
      default:
        log.error("Unknown command: " + options.getCommand());
        break;
    }

    // clean up
    twillRunner.stop();
    curator.close();
  }
}
