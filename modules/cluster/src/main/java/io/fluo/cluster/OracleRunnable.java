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
package io.fluo.cluster;

import java.io.File;

import com.beust.jcommander.JCommander;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.cluster.util.Logging;
import io.fluo.core.impl.Environment;
import io.fluo.core.oracle.OracleServer;
import io.fluo.core.util.UtilWaitThread;
import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 
 * Main run method of Fluo oracle that can be called within
 * a Twill/YARN application or on its own as a Java application
 */
public class OracleRunnable extends AbstractTwillRunnable {

  private static Logger log = LoggerFactory.getLogger(OracleRunnable.class);

  @Override
  public void run() {
    System.out.println("Starting Oracle");
    String[] args = { "-config-dir", "./conf"};
    run(args);
  }

  public void run(String[] args) {
    try {
      RunnableOptions options = new RunnableOptions();
      JCommander jcommand = new JCommander(options, args);

      if (options.help) {
        jcommand.usage();
        System.exit(-1);
      }
      options.validateConfig();

      Logging.init("oracle", options.getConfigDir(), options.getLogOutput());

      FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
      if (!config.hasRequiredOracleProps()) {
        log.error("fluo.properties is missing required properties for oracle");
        System.exit(-1);
      }
      
      Environment env = new Environment(config);
      
      log.info("Oracle configuration:");
      env.getConfiguration().print();

      OracleServer server = new OracleServer(env);
      server.start();

      while (true) {
        UtilWaitThread.sleep(10000);
      }
    } catch (Exception e) {
      System.err.println("Exception running oracle: "+ e.getMessage());
      e.printStackTrace();
    }
  }
  
  @Override
  public void stop() {
    log.info("Stopping Fluo oracle");
  }
  
  public static void main(String[] args) {
    OracleRunnable oracle = new OracleRunnable();
    oracle.run(args);
  }
}

