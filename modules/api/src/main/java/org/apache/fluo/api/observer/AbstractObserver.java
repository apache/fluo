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

package org.apache.fluo.api.observer;

import org.apache.fluo.api.client.TransactionBase;

/**
 * Implemented by users to a watch a {@link org.apache.fluo.api.data.Column} and be notified of
 * changes to the Column via the
 * {@link #process(TransactionBase, org.apache.fluo.api.data.Bytes, org.apache.fluo.api.data.Column)}
 * method. AbstractObserver extends {@link Observer} but provides a default implementation for the
 * {@link #init(Context)} and {@link #close()} method so that they can be optionally implemented by
 * user.
 *
 * @since 1.0.0
 */
public abstract class AbstractObserver implements Observer {

  @Override
  public void init(Context context) throws Exception {}

  @Override
  public void close() {}
}
