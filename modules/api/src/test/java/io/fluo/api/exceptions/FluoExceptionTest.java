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
package io.fluo.api.exceptions;

import org.junit.Assert;
import org.junit.Test;

public class FluoExceptionTest {

  @Test(expected = FluoException.class)
  public void testThrowFluo() {
    throwFluoException();
  }

  @Test
  public void testCatchFluo() {
    try {
      throwFluoException();
      Assert.assertFalse(true);
    } catch (FluoException e) {}
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
    } catch (FluoException e) {}

    try {
      throwCommitException();
      Assert.assertFalse(true);
    } catch (CommitException e) {}
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
    } catch (FluoException e) {}

    try {
      throwAlreadySetException();
      Assert.assertFalse(true);
    } catch (AlreadySetException e) {}
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
