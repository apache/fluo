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

package org.apache.fluo.api.service;

/**
 * Shared interface for Fluo services
 */
public interface FluoService {

  /**
   * Starts service. Waits until service has started before returning.
   * 
   * @throws org.apache.fluo.api.exceptions.FluoException if service fails to start
   */
  void start();

  /**
   * Stops service cleanly. Waits until service has stopped before returning.
   * 
   * @throws org.apache.fluo.api.exceptions.FluoException if service has failed or failure occurs
   *         while stopping.
   */
  void stop();

}
