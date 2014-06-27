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
package org.fluo.yarn;

import java.io.File;
import java.net.URI;
import java.util.Properties;

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

import org.fluo.api.config.WorkerProperties;
import org.fluo.cluster.util.Logging;
import org.fluo.impl.Configuration;
import org.fluo.tools.InitializeTool;

import com.beust.jcommander.JCommander;

/** Tool to start a distributed Fluo workers in YARN
 */
public class WorkerApp implements TwillApplication {
  
  private static Logger log = LoggerFactory.getLogger(WorkerApp.class);
  private AppOptions options;
  private Properties props;
  
  public WorkerApp(AppOptions options, Properties props) {
    this.options = options;
    this.props = props;
  }
       
  public TwillSpecification configure() { 
    int numInstances = Integer.parseInt(props.getProperty(WorkerProperties.WORKER_INSTANCES_PROP, "1"));
    int maxMemoryMB = Integer.parseInt(props.getProperty(WorkerProperties.WORKER_MAX_MEMORY_PROP, "256"));
    
    ResourceSpecification workerResources = ResourceSpecification.Builder.with()
        .setVirtualCores(1)
        .setMemory(maxMemoryMB, SizeUnit.MEGA)
        .setInstances(numInstances).build();
    
    log.info("Starting "+numInstances+" workers with "+maxMemoryMB+"MB of memory");

    MoreFile moreFile = TwillSpecification.Builder.with() 
        .setName("FluoWorker").withRunnable()
        .add(new WorkerRunnable(), workerResources)
        .withLocalFiles()
        .add("./conf/connection.properties", new File(String.format("%s/conf/connection.properties", options.getFluoHome())));

    File confDir = new File(String.format("%s/conf", options.getFluoHome()));
    for (File f : confDir.listFiles()) {
      if (f.isFile() && (f.getName().equals("connection.properties") == false)) {
        moreFile = moreFile.add(String.format("./conf/%s", f.getName()), f);
      }
    }

    return moreFile.apply().anyOrder().build();
  }

  public static void main(String[] args) throws ConfigurationException, Exception {
    
    AppOptions options = new AppOptions();
    JCommander jcommand = new JCommander(options, args);

    if (options.help) {
      jcommand.usage();
      System.exit(-1);
    }
    
    Logging.init("worker", options.getFluoHome()+"/conf", "STDOUT");
    
    Properties props = InitializeTool.loadProps(options.getFluoHome() + "/conf/connection.properties");
    Configuration config = new Configuration(props);
    
    YarnConfiguration yarnConfig = new YarnConfiguration();
    yarnConfig.addResource(new Path(options.getHadoopPrefix()+"/etc/hadoop/core-site.xml"));
        
    TwillRunnerService twillRunner = new YarnTwillRunnerService(yarnConfig, config.getZookeepers()); 
    twillRunner.startAndWait(); 
    
    TwillPreparer preparer = twillRunner.prepare(new WorkerApp(options, props)); 
   
    // Add any observer jars found in lib observers
    File observerDir = new File(options.getFluoHome()+"/lib/observers");
    for (File f : observerDir.listFiles()) {
      String jarPath = "file:"+f.getCanonicalPath();
      log.debug("Adding observer jar "+jarPath+" to YARN app");
      preparer.withResources(new URI(jarPath));
    }
        
    TwillController controller = preparer.start();
    controller.start();
    
    while (controller.isRunning() == false) {
      Thread.sleep(2000);
    }
    System.exit(0);
  }
}
