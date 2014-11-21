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
package io.fluo.stress.trie;

import com.google.common.base.Strings;
import com.google.common.hash.Hashing;

import static com.google.common.base.Preconditions.checkArgument;

/** Utility class that represents trie node
 */
public class Node {
  
  private final Number number; 
  private final int level;
  private final int nodeSize;
  
  static final int HASH_LEN=4;
  
  public Node(Number number, int level, int nodeSize) {
    this.number = number;
    this.level = level;
    this.nodeSize = nodeSize;
  }
  
  public Node(String rowId) {
    String[] rowArgs = rowId.split(":");
    checkArgument(validRowId(rowArgs), "Invalid row id - "+ rowId);
    this.level = Integer.parseInt(rowArgs[0]);
    this.nodeSize = Integer.parseInt(rowArgs[2]);
    this.number = parseNumber(rowArgs[3]);
  }
  
  public Number getNumber() {
    return number;
  }
  
  public int getLevel() {
    return level;
  }
  
  public boolean isRoot() {
    return level == 0;
  }
  
  public int getNodeSize() {
    return nodeSize;
  }
  
  private Number parseNumber(String numStr) {
    if (numStr.equals("root")) {
      return null;
    } else if (numStr.length() == 16) {
      return Long.parseLong(numStr, 16);
    } else {
      return Integer.parseInt(numStr, 16);
    }
  }
  
  private String genHash(){
    long num = (number == null)? 0l : number.longValue();
    int hash = Hashing.murmur3_32().newHasher().putInt(level).putInt(nodeSize).putLong(num).hash().asInt();
    hash = hash & 0x7fffffff;
    //base 36 gives a lot more bins in 4 bytes than hex, but it still human readable which is nice for debugging.
    String hashString = Strings.padStart(Integer.toString(hash, Character.MAX_RADIX), HASH_LEN, '0');
    return hashString.substring(hashString.length() - HASH_LEN);
  }
  
  public String getRowId() {
    if (level == 0) {
      return String.format("00:%s:%02d:root", genHash(), nodeSize); 
    } else { 
      if (number instanceof Integer) {
        return String.format("%02d:%s:%02d:%08x", level, genHash(), nodeSize, number);
      } else {
        return String.format("%02d:%s:%02d:%016x", level, genHash(), nodeSize, number);
      }
    }
  }
  
  public Node getParent() {
    if (level == 1) {
      return new Node(null, 0, nodeSize);
    } else {   
      if (number instanceof Long) {
        int shift = (((64 / nodeSize) - level) * nodeSize) + nodeSize;
        Long parent = (number.longValue() >> shift) << shift;
        return new Node(parent, level-1, nodeSize);
      } else {
        int shift = (((32 / nodeSize) - level) * nodeSize) + nodeSize;
        Integer parent = (number.intValue() >> shift) << shift;
        return new Node(parent, level-1, nodeSize);
      }
    }  
  }
    
  private boolean validRowId(String[] rowArgs) {
    return ((rowArgs.length == 4) && (rowArgs[0] != null) && (rowArgs[1] != null) && (rowArgs[2] != null) && (rowArgs[3] != null));
  }
  
  public static String generateRootId(int nodeSize) {
    return (new Node(null, 0, nodeSize)).getRowId();
  }
}
