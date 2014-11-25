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
package io.fluo.cluster.scan;

import java.util.Collections;
import java.util.List;

import com.beust.jcommander.Parameter;

/**
 * Scan command line options 
 */
public class ScanOptions {
  
  @Parameter(names = "-s", description = "Start row (inclusive) of scan")
  private String startRow;
  
  @Parameter(names = "-e", description = "End row (inclusive) of scan")
  private String endRow;
  
  @Parameter(names = "-c", description = "Columns of scan in comma separated format: <<columnfamily>[:<columnqualifier>]{,<columnfamily>[:<columnqualifier>]}> ")
  private List<String> columns;
  
  @Parameter(names = "-r", description = "Exact row to scan")
  private String exactRow;
  
  @Parameter(names = "-p", description = "Row prefix to scan")
  private String rowPrefix;
    
  @Parameter(names = "-config-dir", description = "Location of Fluo configuration directory", required = true, hidden = true)
  private String configDir;
    
  @Parameter(names = {"-h", "-help", "--help"}, help = true, description = "Prints help")
  public boolean help;
  
  public String getStartRow() {
    return startRow;
  }
  
  public String getEndRow() {
    return endRow;
  }
  
  public String getExactRow() {
    return exactRow;
  }
  
  public String getRowPrefix() {
    return rowPrefix;
  }
  
  public List<String> getColumns() {
    if (columns == null) {
      return Collections.emptyList();
    }
    return columns;
  }
    
  public String getConfigDir() {
    return configDir;
  }
  
  public String getFluoProps() {
    return configDir + "/fluo.properties";
  }
}
