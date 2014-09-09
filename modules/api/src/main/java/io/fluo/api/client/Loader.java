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

/**
 * Interface that needs to be implemented to load data into Fluo
 */
public interface Loader {

  /**
   * Users implement this method to load data into Fluo using
   * the provided transaction.  The transaction will be committed
   * after method returns
   *
   * @param tx Transaction provided for loading data
   * @throws Exception
   */
  public abstract void load(Transaction tx) throws Exception;
}
