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

package org.apache.fluo.core.impl;

import java.io.IOException;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode;
import org.apache.curator.framework.recipes.nodes.PersistentEphemeralNode.Mode;
import org.apache.fluo.accumulo.util.LongUtil;
import org.apache.fluo.accumulo.util.ZookeeperPath;
import org.apache.fluo.core.util.CuratorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A transactor node is marker in Zookeeper that helps clients determine if a transactor has died
 * and its transactions can be rolled back.
 */
public class TransactorNode implements AutoCloseable {

  public enum TrStatus {
    OPEN, CLOSED
  }

  private static final Logger log = LoggerFactory.getLogger(TransactorNode.class);
  private Environment env;
  private PersistentEphemeralNode node;
  private TransactorID tid;
  private TrStatus status;

  /**
   * Creates a transactor node using given transactor id
   * 
   * @param env Environment
   * @param tid Transactor ID used to create node
   */
  public TransactorNode(Environment env, TransactorID tid) {
    this.env = env;
    this.tid = tid;
    node = new PersistentEphemeralNode(env.getSharedResources().getCurator(), Mode.EPHEMERAL,
        getNodePath(), tid.toString().getBytes());
    CuratorUtil.startAndWait(node, 10);
    status = TrStatus.OPEN;
  }

  /**
   * Creates a transactor node using new transactor ID
   * 
   * @param env Environment
   */
  public TransactorNode(Environment env) {
    this(env, new TransactorID(env));
  }

  /**
   * Retrieves current status of node
   */
  public TrStatus getStatus() {
    return status;
  }

  /**
   * Returns the transactorID of this node
   */
  public TransactorID getTransactorID() {
    if (getStatus() == TrStatus.CLOSED) {
      throw new IllegalStateException("TransactorID is closed!");
    }
    return tid;
  }

  /**
   * Closes the transactor node by removing its node in Zookeeper
   */
  @Override
  public void close() {
    status = TrStatus.CLOSED;
    try {
      node.close();
    } catch (IOException e) {
      log.error("Failed to close ephemeral node");
      throw new IllegalStateException(e);
    }
  }

  @VisibleForTesting
  public String getNodePath() {
    return getNodePath(env, tid.getLongID());
  }

  public static String getNodePath(Environment env, Long transactorId) {
    return ZookeeperPath.TRANSACTOR_NODES + "/" + LongUtil.toMaxRadixString(transactorId);
  }
}
