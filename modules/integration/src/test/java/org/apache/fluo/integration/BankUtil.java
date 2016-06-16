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

package org.apache.fluo.integration;

import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.core.impl.Environment;

/**
 * Utility test methods to mimic banking and transfers
 */
public class BankUtil {

  public static final TypeLayer typeLayer = new TypeLayer(new StringEncoder());
  public static final Column BALANCE = typeLayer.bc().fam("account").qual("balance").vis();

  private BankUtil() {}

  public static void transfer(Environment env, String from, String to, int amount) throws Exception {
    TestTransaction tx = new TestTransaction(env);

    int bal1 = tx.get().row(from).col(BALANCE).toInteger();
    int bal2 = tx.get().row(to).col(BALANCE).toInteger();

    tx.mutate().row(from).col(BALANCE).set(bal1 - amount);
    tx.mutate().row(to).col(BALANCE).set(bal2 + amount);

    tx.done();
  }

  public static void setBalance(TestTransaction tx, String user, int amount) {
    tx.mutate().row(user).col(BALANCE).set(amount);
  }

  public static int getBalance(TestTransaction tx, String user) {
    return tx.get().row(user).col(BALANCE).toInteger();
  }
}
