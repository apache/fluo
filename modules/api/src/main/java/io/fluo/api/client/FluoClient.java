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

import java.io.Closeable;

/**
 * Client interface for Fluo. Fluo clients will have shared resources
 * used by all objects created by the client.  Therefore, close() must
 * called when you are finished using a client.
 */
public interface FluoClient extends Closeable {

  public LoaderExecutor newLoaderExecutor();

  public Snapshot newSnapshot();

  public void close();

}