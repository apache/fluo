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

import java.util.List;

class PartitionInfo {

  private final int myGroupId;
  private final int myIdInGroup;
  private final int numGroups;
  private final int myGroupSize;
  private final int numWorkers;
  private final RangeSet myGroupsRanges;

  PartitionInfo(int myId, int myGroupId, int myGroupSize, int totalGroups, int totalWorkers,
      List<TableRange> groupsRanges) {
    this.myIdInGroup = myId;
    this.myGroupId = myGroupId;
    this.myGroupSize = myGroupSize;
    this.numGroups = totalGroups;
    this.numWorkers = totalWorkers;
    this.myGroupsRanges = new RangeSet(groupsRanges);
  }

  /**
   * @return The id for the group this worker is in.
   */
  public int getMyGroupId() {
    return myGroupId;
  }

  /**
   * @return The id for this worker within its group.
   */
  public int getMyIdInGroup() {
    return myIdInGroup;
  }

  /**
   * @return The total number of workers groups there are.
   */
  public int getNumGroups() {
    return numGroups;
  }

  /**
   * @return The number of workers in the group this workers is in.
   */
  public int getMyGroupSize() {
    return myGroupSize;
  }

  /**
   * @return the total number of workers.
   */
  public int getNumWorkers() {
    return numWorkers;
  }

  /**
   * @return the table ranges associated with the group this workers is in.
   */
  public RangeSet getMyGroupsRanges() {
    return myGroupsRanges;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof PartitionInfo) {
      PartitionInfo other = (PartitionInfo) o;
      return other.myGroupId == myGroupId && other.myIdInGroup == myIdInGroup
          && other.numGroups == numGroups && other.myGroupSize == myGroupSize
          && other.numWorkers == numWorkers && other.myGroupsRanges.equals(myGroupsRanges);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format(
        "workers:%d  groups:%d  groupSize:%d  groupId:%d  idInGroup:%d  #tablets:%d", numWorkers,
        numGroups, myGroupSize, myGroupId, myIdInGroup, myGroupsRanges.size());
  }
}
