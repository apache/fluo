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

package org.apache.fluo.core.worker.finder.hash;

class TabletData {
  long retryTime = 0;
  long sleepTime = 0;

  public void updateScanCount(int count, long maxSleep) {
    if (count == 0) {
      // remember if a tablet is empty an do not retry it for a bit... the more times empty, the
      // longer the retry
      retryTime = sleepTime + System.currentTimeMillis();
      if (sleepTime == 0) {
        sleepTime = 100;
      } else {
        if (sleepTime < maxSleep) {
          sleepTime = (long) (1.5 * sleepTime) + (long) (sleepTime * Math.random());
        }
      }
    } else {
      retryTime = 0;
      sleepTime = 0;
    }
  }
}
