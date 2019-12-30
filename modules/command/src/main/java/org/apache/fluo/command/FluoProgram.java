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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.collect.Iterables;

public class FluoProgram {

  public static void main(String[] args) {
    List<FluoCommand> fluoCommands = Arrays.asList(new FluoConfig(), new FluoExec(),
        new FluoGetJars(), new FluoInit(), new FluoList(), new FluoOracle(), new FluoRemove(),
        new FluoScan(), new FluoStatus(), new FluoWait(), new FluoWorker());
    try {
      runFluoCommand(fluoCommands, args);
    } catch (FluoCommandException | ParameterException e) {
      System.exit(1);
    }
  }

  public static void runFluoCommand(List<FluoCommand> fluoCommands, String[] args) {
    JCommander.Builder jCommanderBuilder = JCommander.newBuilder();
    fluoCommands.forEach(jCommanderBuilder::addCommand);
    JCommander jcommand = jCommanderBuilder.build();

    try {
      jcommand.parse(args);
    } catch (ParameterException e) {
      System.err.println(e.getMessage());
      String commandName = Optional.ofNullable(jcommand.getParsedCommand()).orElse("");
      JCommander parsedJCommandOrProgram =
          Optional.ofNullable(jcommand.findCommandByAlias(commandName)).orElse(jcommand);
      parsedJCommandOrProgram.setProgramName(String.format("fluo %s", commandName));
      parsedJCommandOrProgram.usage();
      throw e;
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
      throw e;
    }
  }
}
