/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.accumulo.iterators;

import java.io.IOException;

import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.security.Authorizations;

public class TestIteratorEnv implements IteratorEnvironment {

  private IteratorScope scope;
  private boolean fullMajc = true;

  TestIteratorEnv(IteratorScope scope) {
    this.scope = scope;
  }

  public TestIteratorEnv(IteratorScope scope, boolean fullMajc) {
    this.scope = scope;
    this.fullMajc = fullMajc;
  }

  @Override
  public SortedKeyValueIterator<Key, Value> reserveMapFileReader(String mapFileName)
      throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AccumuloConfiguration getConfig() {
    throw new UnsupportedOperationException();
  }

  @Override
  public IteratorScope getIteratorScope() {
    return scope;
  }

  @Override
  public boolean isFullMajorCompaction() {
    if (scope == IteratorScope.majc)
      return fullMajc;
    return false;
  }

  @Override
  public void registerSideChannel(SortedKeyValueIterator<Key, Value> iter) {
    throw new UnsupportedOperationException();
  }

  // this is a new method added in Accumulo 1.7.0. Can not add @Override because it will not compile
  // against 1.6.X
  public Authorizations getAuthorizations() {
    return null;
  }
}
