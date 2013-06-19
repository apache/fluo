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
package org.apache.accumulo.accismus;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

import org.apache.accumulo.core.security.ColumnVisibility;

/**
 * 
 */
public class Column {
  
  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility(new byte[0]);

  private byte[] family;
  private byte[] qualifier;
  private ColumnVisibility visibility = EMPTY_VIS;
  
  private static byte[] toBytes(String s) {
    try {
      return s.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  public int hashCode() {
    return Arrays.hashCode(family) + Arrays.hashCode(qualifier) + visibility.hashCode();
  }
  
  public boolean equals(Object o) {
    if (o instanceof Column) {
      Column oc = (Column) o;
      
      return Arrays.equals(family, oc.family) && Arrays.equals(qualifier, oc.qualifier) && visibility.equals(oc.visibility);
    }
    
    return false;
  }
  
  public Column(String family, String qualifier) {
    // a limitation of this prototype...
    if (family.contains(Constants.SEP) || qualifier.contains(Constants.SEP)) {
      throw new IllegalArgumentException("columns can not contain : in prototype");
    }
    
    this.family = toBytes(family);
    this.qualifier = toBytes(qualifier);
  }
  
  public Column(byte[] family, byte[] qualifier) {
    this.family = family;
    this.qualifier = qualifier;
  }

  public byte[] getFamily() {
    return family;
  }
  
  public byte[] getQualifier() {
    return qualifier;
  }

  public Column setVisibility(ColumnVisibility cv) {
    this.visibility = cv;
    return this;
  }
  
  public ColumnVisibility getVisibility() {
    return visibility;
  }

  public String toString() {
    try {
      return new String(family, "UTF-8") + " " + new String(qualifier, "UTF-8") + " " + visibility;
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }
}
