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

import static org.apache.fluo.command.FluoCommandType.CONFIG;
import static org.apache.fluo.command.FluoCommandType.EXEC;
import static org.apache.fluo.command.FluoCommandType.GET_JARS;
import static org.apache.fluo.command.FluoCommandType.INIT;
import static org.apache.fluo.command.FluoCommandType.LIST;
import static org.apache.fluo.command.FluoCommandType.ORACLE;
import static org.apache.fluo.command.FluoCommandType.REMOVE;
import static org.apache.fluo.command.FluoCommandType.SCAN;
import static org.apache.fluo.command.FluoCommandType.STATUS;
import static org.apache.fluo.command.FluoCommandType.WAIT;
import static org.apache.fluo.command.FluoCommandType.WORKER;

public class FluoProgram {
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
    JCommander jcommand = JCommander.newBuilder().addCommand(CONFIG.getCommandName(), fluoConfig)
        .addCommand(EXEC.getCommandName(), fluoExec)
        .addCommand(GET_JARS.getCommandName(), fluoGetJars)
        .addCommand(INIT.getCommandName(), fluoInit).addCommand(LIST.getCommandName(), fluoList)
        .addCommand(ORACLE.getCommandName(), fluoOracle)
        .addCommand(REMOVE.getCommandName(), fluoRemove).addCommand(SCAN.getCommandName(), fluoScan)
        .addCommand(STATUS.getCommandName(), fluoStatus).addCommand(WAIT.getCommandName(), fluoWait)
        .addCommand(WORKER.getCommandName(), fluoWorker).build();

    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      String commandName = Optional.ofNullable(jcommand.getParsedCommand()).orElse("");
      JCommander parsedCommandOrProgram =
          Optional.ofNullable(jcommand.findCommandByAlias(commandName)).orElse(jcommand);
      parsedCommandOrProgram.setProgramName(String.format("fluo %s", commandName));
      parsedCommandOrProgram.usage();
      System.exit(1);
      return;
    }

    FluoCommandType parsedCommandType = FluoCommandType.valueOf(jcommand.getParsedCommand());

    FluoCommand parsedCommand;
    switch (parsedCommandType) {
      case CONFIG:
        parsedCommand = fluoConfig;
        break;
      case EXEC:
        parsedCommand = fluoExec;
        break;
      case GET_JARS:
        parsedCommand = fluoGetJars;
        break;
      case INIT:
        parsedCommand = fluoInit;
        break;
      case LIST:
        parsedCommand = fluoList;
        break;
      case ORACLE:
        parsedCommand = fluoOracle;
        break;
      case REMOVE:
        parsedCommand = fluoRemove;
        break;
      case SCAN:
        parsedCommand = fluoScan;
        break;
      case STATUS:
        parsedCommand = fluoStatus;
        break;
      case WAIT:
        parsedCommand = fluoWait;
        break;
      case WORKER:
        parsedCommand = fluoWorker;
        break;
      default:
        System.err
            .println(String.format("Unrecognized command %s", parsedCommandType.getCommandName()));
        System.exit(1);
        return;
    }

    String programName = String.format("fluo %s", parsedCommandType.getCommandName());
    JCommander command = jcommand.findCommandByAlias(parsedCommandType.getCommandName());
    command.setProgramName(programName);

    if (parsedCommand.isHelp()) {
      command.usage();
      return;
    }

    try {
      parsedCommand.execute();
    } catch (FluoCommandException e) {
      System.err.println(String.format("%s failed - %s", programName, e.getMessage()));
      System.exit(1);
    }
  }
}
