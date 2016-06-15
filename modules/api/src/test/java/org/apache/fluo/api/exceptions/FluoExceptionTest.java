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

package org.apache.fluo.api.exceptions;

import org.junit.Assert;
import org.junit.Test;

public class FluoExceptionTest {

  @Test
  public void testFluoExceptionConstructors() {
    FluoException e = new FluoException();
    Assert.assertEquals(null, e.getMessage());

    e = new FluoException("msg1");
    Assert.assertEquals("msg1", e.getMessage());

    e = new FluoException("msg2", new IllegalArgumentException("msg3"));
    Assert.assertEquals("msg2", e.getMessage());

    e = new FluoException(new IllegalArgumentException("msg4"));
    Assert.assertEquals("java.lang.IllegalArgumentException: msg4", e.getMessage());
  }

  @Test(expected = FluoException.class)
  public void testThrowFluo() {
    throwFluoException();
  }

  @Test
  public void testCatchFluo() {
    try {
      throwFluoException();
      Assert.assertFalse(true);
    } catch (FluoException e) {
    }
  }

  @Test(expected = CommitException.class)
  public void testThrowCommit() {
    throwCommitException();
  }

  @Test
  public void testCatchCommit() {
    try {
      throwCommitException();
      Assert.assertFalse(true);
    } catch (FluoException e) {
    }

    try {
      throwCommitException();
      Assert.assertFalse(true);
    } catch (CommitException e) {
    }
  }

  @Test(expected = AlreadySetException.class)
  public void testThrowAlreadySet() {
    throwAlreadySetException();
  }

  @Test
  public void testCatchAlreadySet() {
    try {
      throwAlreadySetException();
      Assert.assertFalse(true);
    } catch (FluoException e) {
    }

    try {
      throwAlreadySetException();
      Assert.assertFalse(true);
    } catch (AlreadySetException e) {
    }
  }

  private void throwFluoException() throws FluoException {
    throw new FluoException();
  }

  private void throwCommitException() throws CommitException {
    throw new CommitException();
  }

  private void throwAlreadySetException() throws AlreadySetException {
    throw new AlreadySetException();
  }
}
