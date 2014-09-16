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
package io.fluo.api.client;

import io.fluo.api.data.Bytes;
import io.fluo.api.data.Column;

/**
 * Enables users to read and write to a Fluo table at certain point in time.
 * See {@link io.fluo.api.client.SnapshotBase} for available read methods.
 */
public interface TransactionBase extends SnapshotBase {
  
  public void setWeakNotification(Bytes row, Column col);

  public void set(Bytes row, Column col, Bytes value);
  
  public void delete(Bytes row, Column col);
}
