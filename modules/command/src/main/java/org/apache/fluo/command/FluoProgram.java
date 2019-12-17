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

import java.util.Optional;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Iterables;

public class FluoProgram {

  private static final String CONFIG = "config";
  private static final String EXEC = "exec";
  private static final String GET_JARS = "get-jars";
  private static final String INIT = "init";
  private static final String LIST = "list";
  private static final String ORACLE = "oracle";
  private static final String REMOVE = "remove";
  private static final String SCAN = "scan";
  private static final String STATUS = "status";
  private static final String WAIT = "wait";
  private static final String WORKER = "worker";

  public static void main(String[] args) {
    FluoConfig fluoConfig = new FluoConfig();
    FluoExec fluoExec = new FluoExec();
    FluoGetJars fluoGetJars = new FluoGetJars();
    FluoInit fluoInit = new FluoInit();
    FluoList fluoList = new FluoList();
    FluoOracle fluoOracle = new FluoOracle();
    FluoRemove fluoRemove = new FluoRemove();
    FluoScan fluoScan = new FluoScan();
    FluoStatus fluoStatus = new FluoStatus();
    FluoWait fluoWait = new FluoWait();
    FluoWorker fluoWorker = new FluoWorker();
    JCommander jcommand = JCommander.newBuilder().addCommand(CONFIG, fluoConfig)
        .addCommand(EXEC, fluoExec).addCommand(GET_JARS, fluoGetJars).addCommand(INIT, fluoInit)
        .addCommand(LIST, fluoList).addCommand(ORACLE, fluoOracle).addCommand(REMOVE, fluoRemove)
        .addCommand(SCAN, fluoScan).addCommand(STATUS, fluoStatus).addCommand(WAIT, fluoWait)
        .addCommand(WORKER, fluoWorker).build();

    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      String commandName = Optional.ofNullable(jcommand.getParsedCommand()).orElse("");
      JCommander parsedJCommandOrProgram =
          Optional.ofNullable(jcommand.findCommandByAlias(commandName)).orElse(jcommand);
      parsedJCommandOrProgram.setProgramName(String.format("fluo %s", commandName));
      parsedJCommandOrProgram.usage();
      System.exit(1);
      return;
    }

    String parsedCommandType = jcommand.getParsedCommand();
    JCommander parsedJCommand = jcommand.findCommandByAlias(parsedCommandType);
    String programName = String.format("fluo %s", parsedCommandType);
    parsedJCommand.setProgramName(programName);
    FluoCommand parsedFluoCommand =
        (FluoCommand) Iterables.getOnlyElement(parsedJCommand.getObjects());

    if (parsedFluoCommand.isHelp()) {
      parsedJCommand.usage();
      return;
    }

    try {
      parsedFluoCommand.execute();
    } catch (FluoCommandException e) {
      System.err.println(String.format("%s failed - %s", programName, e.getMessage()));
      System.exit(1);
    }
  }
}
