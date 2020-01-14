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

import java.io.PrintStream;
import java.util.Collections;

import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class FluoProgramTest {
  @Parameters(commandNames = "test")
  class MockFluoCommand implements FluoCommand {

    boolean help = false;
    boolean throwException = false;
    private boolean executed = false;

    @Override
    public void execute() throws FluoCommandException {
      if (throwException) {
        throw new FluoCommandException();
      } else {
        executed = true;
      }
    }

    @Override
    public boolean isHelp() {
      return help;
    }

    public boolean isExecuted() {
      return executed;
    }
  }

  private MockFluoCommand mockFluoCommand;
  private static PrintStream outPS;
  private static PrintStream errPS;

  @BeforeClass
  public static void disablePrinting() {
    outPS = System.out;
    errPS = System.err;
    // This will hide usage and error logs when running tests
    try (PrintStream ps = new PrintStream(new NullOutputStream())) {
      System.setOut(ps);
      System.setErr(ps);
    }
  }

  @AfterClass
  public static void restorePrinting() {
    System.setOut(outPS);
    System.setErr(errPS);
  }

  @Before
  public void setUp() {
    mockFluoCommand = new MockFluoCommand();
  }

  @Test(expected = ParameterException.class)
  public void testUnparsableCommand() {
    FluoProgram.runFluoCommand(Collections.singletonList(new MockFluoCommand()),
        new String[] {"invalid", "command"});
  }

  @Test
  public void testHelpCommand() {
    mockFluoCommand.help = true;

    FluoProgram.runFluoCommand(Collections.singletonList(mockFluoCommand), new String[] {"test"});

    assertFalse(mockFluoCommand.isExecuted());
  }

  @Test
  public void testExecutedCommand() {
    FluoProgram.runFluoCommand(Collections.singletonList(mockFluoCommand), new String[] {"test"});

    assertTrue(mockFluoCommand.isExecuted());
  }

  @Test(expected = FluoCommandException.class)
  public void testExecutionError() {
    mockFluoCommand.throwException = true;

    FluoProgram.runFluoCommand(Collections.singletonList(mockFluoCommand), new String[] {"test"});
  }
}
