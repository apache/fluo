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

import java.io.File;
import java.util.Map.Entry;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import io.fluo.api.client.FluoClient;
import io.fluo.api.client.FluoFactory;
import io.fluo.api.client.Snapshot;
import io.fluo.api.config.FluoConfiguration;
import io.fluo.api.config.ScannerConfiguration;
import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;
import io.fluo.api.data.Span;
import io.fluo.api.iterator.ColumnIterator;
import io.fluo.api.iterator.RowIterator;
import io.fluo.core.mini.MiniFluoImpl;
import org.slf4j.LoggerFactory;

/**
 * Scans and prints Fluo table 
 */
public class Scan {
  
  public static ScannerConfiguration buildScanConfig(ScanOptions options) {
    ScannerConfiguration scanConfig = new ScannerConfiguration();
    
    if ((options.getExactRow() != null) && ((options.getStartRow() != null) || (options.getEndRow() != null) || (options.getRowPrefix() != null))) {
      throw new IllegalArgumentException("You cannot specify an exact row with a start/end row or row prefix!");
    }
    
    if ((options.getRowPrefix() != null) && ((options.getStartRow() != null) || (options.getEndRow() != null) || (options.getExactRow() != null))) {
      throw new IllegalArgumentException("You cannot specify an prefix row with a start/end row or exact row!");
    }
    
    // configure span of scanner
    if (options.getExactRow() != null) {
      scanConfig.setSpan(Span.exact(options.getExactRow()));
    } else if (options.getRowPrefix() != null) {
      scanConfig.setSpan(Span.prefix(options.getRowPrefix()));
    } else {
      if ((options.getStartRow() != null) && (options.getEndRow() != null)) {
        scanConfig.setSpan(new Span(options.getStartRow(), true, options.getEndRow(), true));
      } else if (options.getStartRow() != null) { 
        scanConfig.setSpan(new Span(Bytes.wrap(options.getStartRow()), true, Bytes.EMPTY, true));
      } else if (options.getEndRow() != null) {
        scanConfig.setSpan(new Span(Bytes.EMPTY, true, Bytes.wrap(options.getEndRow()), true));
      }
    }

    // configure columns of scanner
    for (String column : options.getColumns()) {
      String[] colFields = column.split(":");
      if (colFields.length == 1) {
        scanConfig.fetchColumnFamily(Bytes.wrap(colFields[0]));
      } else if (colFields.length == 2) {
        scanConfig.fetchColumn(Bytes.wrap(colFields[0]), Bytes.wrap(colFields[1]));
      } else {
        throw new IllegalArgumentException("Failed to scan!  Column '" + column + "' has too many fields (indicated by ':')");
      }
    }
    
    return scanConfig;
  }
  
  public static void main(String[] args) throws Exception {
    
    ScanOptions options = new ScanOptions();
    JCommander jcommand = new JCommander(options);
    jcommand.setProgramName("fluo scan");
    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      jcommand.usage();
      System.exit(-1);
    }
    
    if (options.help) {
      jcommand.usage();
      System.exit(0);
    }
    
    Logger root = (Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.ERROR);

    FluoConfiguration config = new FluoConfiguration(new File(options.getFluoProps()));
    FluoConfiguration chosenConfig = null;

    if (config.hasRequiredClientProps()) {
      chosenConfig = config;
      System.out.println("Scanning '"+config.getAccumuloTable() + "' table of Fluo instance (" + config.getZookeepers() + ")");
    } else {
      File miniDataDir = new File(config.getMiniDataDir());
      if (miniDataDir.exists()) {
        System.err.println("No Fluo instance found to connect with!  Client properties are not set in fluo.properties and a MiniAccumuloCluster is not running at "+miniDataDir.getAbsolutePath());
        System.exit(-1);
      }

      if (!config.hasRequiredMiniFluoProps()) {
        System.err.println("Fluo properties are not configured correctly!");
        System.exit(-1);
      }

      File clientPropsFile = new File(MiniFluoImpl.clientPropsPath(config));
      if (!clientPropsFile.exists()) {
        System.err.println("MiniFluo client.properties do not exist at " + clientPropsFile.getAbsolutePath());
        System.exit(-1);
      }
      chosenConfig = new FluoConfiguration(clientPropsFile);
      System.out.println("Scanning '" + chosenConfig.getAccumuloTable() + "' table of MiniFluo instance (" + config.getMiniDataDir()+ ")");
    }
        
    try (FluoClient client = FluoFactory.newClient(chosenConfig)) {
      try (Snapshot s = client.newSnapshot()) {
        
        ScannerConfiguration scanConfig = null;
        try {
          scanConfig = buildScanConfig(options);
        } catch (IllegalArgumentException e) {
          System.err.println(e.getMessage());
          System.exit(-1);
        }
        
        RowIterator iter = s.get(scanConfig);
        
        if (iter.hasNext() == false) {
          System.out.println("\nNo data found\n");
        }
        
        while (iter.hasNext() && !System.out.checkError()) {
          Entry<Bytes,ColumnIterator> rowEntry = iter.next();
          ColumnIterator citer = rowEntry.getValue();
          while (citer.hasNext() && !System.out.checkError()) {
            Entry<Column,Bytes> colEntry = citer.next();
            System.out.println(rowEntry.getKey() + " " + colEntry.getKey() + "\t" + colEntry.getValue());
          }
        }
      }
    }
    System.exit(0);
  }
}
