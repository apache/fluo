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

package org.apache.fluo.cluster.yarn;

import java.util.Collection;

import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunResources;

/**
 * Twill Utility classes
 */
public class TwillUtil {

  private TwillUtil() {}

  public static int numRunning(TwillController controller, String runnableName) {
    return controller.getResourceReport().getRunnableResources(runnableName).size();
  }

  public static void printResources(Collection<TwillRunResources> resourcesList) {
    System.out.println("Instance  Cores  MaxMemory  Container ID                             Host");
    System.out.println("--------  -----  ---------  ------------                             ----");
    for (TwillRunResources resources : resourcesList) {
      System.out.format("%-9s %-6s %4s MB    %-40s %s\n", resources.getInstanceId(),
          resources.getVirtualCores(), resources.getMemoryMB(), resources.getContainerId(),
          resources.getHost());
    }
  }
}
