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

import org.apache.fluo.api.client.FluoAdmin;
import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.exceptions.FluoException;
import org.apache.fluo.core.client.FluoAdminImpl;

public class FluoRemove {

  public static void main(String[] args) {

    CommonOpts opts = CommonOpts.parse("fluo remove", args);

    FluoConfiguration config = CommandUtil.resolveFluoConfig();
    config.setApplicationName(opts.getApplicationName());
    opts.overrideFluoConfig(config);
    config = FluoAdminImpl.mergeZookeeperConfig(config);

    try (FluoAdminImpl admin = new FluoAdminImpl(config)) {
      System.out.println("Removing Fluo '" + config.getApplicationName());
      admin.remove();
      System.out.println("Remove is complete.");
    } catch (FluoException e) {
      System.err.println(e.getMessage());
      System.exit(-1);
    } catch (Exception e) {
      System.out.println("Remove failed due to the following exception:");
      e.printStackTrace();
      System.exit(-1);
    }
  }
}
