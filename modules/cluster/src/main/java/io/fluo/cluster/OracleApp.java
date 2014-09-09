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

import com.beust.jcommander.JCommander;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.util.Logging;
import io.fluo.core.impl.Environment;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.ResourceSpecification.SizeUnit;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.TwillSpecification;
import org.apache.twill.api.TwillSpecification.Builder.MoreFile;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool to start a Fluo oracle in YARN
 */
public class OracleApp implements TwillApplication {

  private static Logger log = LoggerFactory.getLogger(OracleApp.class);
  private OracleAppOptions options;
  private FluoConfiguration config;

  public OracleApp(OracleAppOptions options, FluoConfiguration config) {
    this.options = options;
    this.config = config;
  }

  public TwillSpecification configure() {
    int maxMemoryMB = config.getOracleMaxMemory();

    log.info("Starting a fluo oracle with "+maxMemoryMB+"MB of memory");

    ResourceSpecification oracleResources = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(maxMemoryMB, SizeUnit.MEGA)
        .setInstances(options.getNumberOfOracles()).build();

    MoreFile moreFile = TwillSpecification.Builder.with()
        .setName("FluoOracle").withRunnable()
        .add(new OracleRunnable(), oracleResources)
        .withLocalFiles()
        .add("./conf/fluo.properties", new File(String.format("%s/conf/fluo.properties", options.getFluoHome())));

    File confDir = new File(String.format("%s/conf", options.getFluoHome()));
    for (File f : confDir.listFiles()) {
      if (f.isFile() && (f.getName().equals("fluo.properties") == false)) {
        log.info("Adding config file - "+f.getName());
        moreFile = moreFile.add(String.format("./conf/%s", f.getName()), f);
      }
    }

    return moreFile.apply().anyOrder().build();
  }

  public static void main(String[] args) throws ConfigurationException, Exception {

    OracleAppOptions options = new OracleAppOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.displayHelp()) {
      jcommand.usage();
      System.exit(-1);
    }

    Logging.init("oracle", options.getFluoHome()+"/conf", "STDOUT");

    File configFile = new File(options.getFluoHome() + "/conf/fluo.properties");
    FluoConfiguration config = new FluoConfiguration(configFile);
    if (!config.hasRequiredOracleProps()) {
      log.error("fluo.properties is missing required properties for oracle");
      System.exit(-1);
    }
    Environment env = new Environment(config);

    YarnConfiguration yarnConfig = new YarnConfiguration();
    yarnConfig.addResource(new Path(options.getHadoopPrefix()+"/etc/hadoop/core-site.xml"));
    yarnConfig.addResource(new Path(options.getHadoopPrefix()+"/etc/hadoop/yarn-site.xml"));

    TwillRunnerService twillRunner = new YarnTwillRunnerService(yarnConfig, env.getZookeepers());
    twillRunner.startAndWait();

    TwillPreparer preparer = twillRunner.prepare(new OracleApp(options, config));

    TwillController controller = preparer.start();
    controller.start();

    while (controller.isRunning() == false)
      Thread.sleep(2000);

    env.close();
    System.exit(0);
  }
}
