/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.accismus.impl;

import java.util.ArrayList;
import java.util.Collections;

/**
 * 
 */
public class ColumnUtil {
  public static final long PREFIX_MASK = 0xe000000000000000l;
  public static final long WRITE_PREFIX = 0x6000000000000000l;
  public static final long DEL_LOCK_PREFIX = 0x4000000000000000l;
  public static final long LOCK_PREFIX = 0x2000000000000000l;
  public static final long ACK_PREFIX = 0xc000000000000000l;
  public static final long DATA_PREFIX = 0xa000000000000000l;
  
  public static final long TIMESTAMP_MASK = 0x1fffffffffffffffl;
  
  public static void main(String[] args) {
    ArrayList<Long> list = new ArrayList<Long>();
    
    list.add(DEL_LOCK_PREFIX);
    list.add(LOCK_PREFIX);
    list.add(WRITE_PREFIX);
    list.add(DATA_PREFIX);
    list.add(ACK_PREFIX);
    
    Collections.sort(list, Collections.reverseOrder());
    
    for (Long l : list) {
      if ((l & PREFIX_MASK) == DEL_LOCK_PREFIX)
        System.out.println("DEL_LOCK_PREFIX");
      if ((l & PREFIX_MASK) == LOCK_PREFIX)
        System.out.println("LOCK_PREFIX");
      if ((l & PREFIX_MASK) == DATA_PREFIX)
        System.out.println("DATA_PREFIX");
      if ((l & PREFIX_MASK) == WRITE_PREFIX)
        System.out.println("WRITE_PREFIX");
      if ((l & PREFIX_MASK) == ACK_PREFIX)
        System.out.println("ACK_PREFIX");
    }
    
  }
  
}
