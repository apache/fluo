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

package org.apache.fluo.core.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class HostUtil {

  private HostUtil() {}

  public static String getHostName() throws IOException {
    Process p = Runtime.getRuntime().exec("hostname");
    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    String result = reader.readLine();
    try {
      int exitCode = p.waitFor();
      if (exitCode != 0) {
        throw new IOException("Non-zero return from hostname process: " + result);
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
    return result;
  }

}
