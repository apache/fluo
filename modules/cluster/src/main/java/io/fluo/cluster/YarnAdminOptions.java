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

public class YarnAdminOptions {
    
  @Parameter(names = "-fluo-conf", description = "Location of fluo config dir", required = true)
  protected String fluoConf;
  
  @Parameter(names = "-fluo-lib", description = "Location of fluo lib dir", required = true)
  protected String fluoLib;

  @Parameter(names = "-hadoop-prefix", description = "Location of hadoop prefix", required = true)
  protected String hadoopPrefix;
  
  @Parameter(names = "-command", required = true) 
  protected String command;

  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  protected boolean help;

  public String getFluoConf() {
    return fluoConf;
  }
  
  public String getFluoLib() {
    return fluoLib;
  }
  
  public String getHadoopPrefix() {
    return hadoopPrefix;
  }
  
  public String getCommand() {	
    return command;
  }

  public boolean displayHelp() {
    return help;
  }
}
