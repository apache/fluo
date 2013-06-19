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

/**
 * 
 */
public class Column {
  public String family;
  public String qualifier;
  
  public int hashCode() {
    return family.hashCode() + qualifier.hashCode();
  }
  
  public boolean equals(Object o) {
    if (o instanceof Column) {
      Column oc = (Column) o;
      return family.equals(oc.family) && qualifier.equals(oc.qualifier);
    }
    
    return false;
  }
  
  public Column(String family, String qualifier) {
    // a limitation of this prototype...
    if (family.contains(Constants.SEP) || qualifier.contains(Constants.SEP)) {
      throw new IllegalArgumentException("columns can not contain : in prototype");
    }
    this.family = family;
    this.qualifier = qualifier;
  }
  
  public String toString() {
    return family + " " + qualifier;
  }
}
