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

import com.beust.jcommander.Parameter;

public class AppOptions {
  
  @Parameter(names = "-fluo-home", description = "Location of fluo home", required = true)
  protected String fluoHome;

  @Parameter(names = "-hadoop-prefix", description = "Location of hadoop prefix", required = true)
  protected String hadoopPrefix;
  
  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  protected boolean help;

  public String getFluoHome() {
    return fluoHome;
  }
  
  public String getHadoopPrefix() {
    return hadoopPrefix;
  }

  public boolean displayHelp() {
    return help;
  }
}
