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

package org.apache.fluo.accumulo.iterators;

/**
 * This class buffers Keys that all have the same row+column.  Internally 
 * it only stores one Key, a list of timestamps and a list of values.  At iteration 
 * time it materializes each Key+Value.
 */
class ColumnBuffer {
  /**
   * When empty, the first key added sets the row+column.  After this all keys
   * added must have the same row+column.
   */
  public void add(Key k, Value v){
    //TODO
  }
  
  /**
   * Clears the dest ColumnBuffer and inserts all entries in destwhere the timestamp passes 
   * the timestampTest.
   */
  public void copyTo(ColumnBuffer dest, LongPredicate timestampTest){
    //TODO
  }

  public void clear(){
    //TODO
  }

  public int size(){
    //TODO
  }
  
  public Key getKey(int pos){
    //TODO
  }
  
  public Value getValue(int pos){
    //TODO
  }
}