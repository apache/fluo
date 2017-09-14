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

package org.apache.fluo.api.client;

import org.apache.fluo.api.config.FluoConfiguration;
import org.apache.fluo.api.config.SimpleConfiguration;

/**
 * Provides methods for initializing and administering a Fluo application.
 * 
 * @since 1.0.0
 */
public interface FluoAdmin extends AutoCloseable {

  /**
   * Specifies destructive Fluo initialization options that can not be set using
   * {@link FluoConfiguration}. The reason the options in this class are not supported in
   * {@link FluoConfiguration} is that allowing them to be saved in a config file may lead to
   * inadvertent clearing of a Fluo instance.
   *
   * @since 1.0.0
   */
  class InitializationOptions {

    private boolean clearZookeeper = false;
    private boolean clearTable = false;

    /**
     * Clears zookeeper root (if exists) specified by
     * {@value FluoConfiguration#CONNECTION_ZOOKEEPERS_PROP}. Default is false.
     */
    public InitializationOptions setClearZookeeper(boolean clearZookeeper) {
      this.clearZookeeper = clearZookeeper;
      return this;
    }

    public boolean getClearZookeeper() {
      return clearZookeeper;
    }

    /**
     * Clears accumulo table (if exists) specified by {@value FluoConfiguration#ACCUMULO_TABLE_PROP}
     * . Default is false.
     */
    public InitializationOptions setClearTable(boolean clearTable) {
      this.clearTable = clearTable;
      return this;
    }

    public boolean getClearTable() {
      return clearTable;
    }
  }

  /**
   * Exception that is thrown if Fluo application was already initialized. An application is already
   * initialized if a directory with same name as application exists at the chroot directory set by
   * the property fluo.client.zookeeper.connect. If this directory can be cleared, set
   * {@link InitializationOptions#setClearTable(boolean)} to true
   *
   * @since 1.0.0
   */
  class AlreadyInitializedException extends Exception {

    private static final long serialVersionUID = 1L;

    public AlreadyInitializedException(String msg) {
      super(msg);
    }

    public AlreadyInitializedException() {
      super();
    }
  }

  /**
   * Exception that is thrown if Accumulo table (set by fluo.admin.accumulo.table) exists during
   * initialization. If this table can be cleared, set
   * {@link InitializationOptions#setClearZookeeper(boolean)} to true
   *
   * @since 1.0.0
   */
  class TableExistsException extends Exception {

    private static final long serialVersionUID = 1L;

    public TableExistsException(String msg) {
      super(msg);
    }

    public TableExistsException() {
      super();
    }
  }

  /**
   * Initializes Fluo application and stores shared configuration in Zookeeper. Shared configuration
   * consists of all properties except those with
   * {@value org.apache.fluo.api.config.FluoConfiguration#CONNECTION_PREFIX} prefix. Throws
   * {@link AlreadyInitializedException} if Fluo application was already initialized in Zookeeper.
   * If you want to initialize Zookeeper again, set
   * {@link InitializationOptions#setClearZookeeper(boolean)} to true. Throws
   * {@link TableExistsException} if Accumulo table exists. If you want to clear table, set
   * {@link InitializationOptions#setClearTable(boolean)} to true.
   */
  void initialize(InitializationOptions opts)
      throws AlreadyInitializedException, TableExistsException;

  /**
   * Updates shared configuration in Zookeeper. Shared configuration consists of all properties
   * except those with {@value org.apache.fluo.api.config.FluoConfiguration#CONNECTION_PREFIX}
   * prefix. This method is called if a user has previously called
   * {@link #initialize(InitializationOptions)} but wants changes to shared configuration updated in
   * Zookeeper.
   * 
   * <p>
   * During this method Observers are reinitialized using configuration passed to FluoAdmin and not
   * existing shared configuration stored in zookeeper. So make sure all config needed by observers
   * is present.
   */
  void updateSharedConfig();

  /**
   * @return SimpleConfiguration containing connection-specific configuration passed to FluoFactory
   * @since 1.2.0
   */
  SimpleConfiguration getConnectionConfig();

  /**
   * @return SimpleConfiguration containing application-specific configuration stored in Zookeeper
   * @since 1.2.0
   */
  SimpleConfiguration getApplicationConfig();

  @Override
  void close();
}
