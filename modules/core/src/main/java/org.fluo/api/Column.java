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
package org.fluo.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import org.fluo.impl.ByteUtil;

/**
 * 
 */
public class Column implements Writable {
  
  private static final ColumnVisibility EMPTY_VIS = new ColumnVisibility(new byte[0]);

  private ByteSequence family;
  private ByteSequence qualifier;
  private ColumnVisibility visibility = EMPTY_VIS;

  public int hashCode() {
    return family.hashCode() + qualifier.hashCode() + visibility.hashCode();
  }
  
  public boolean equals(Object o) {
    if (o instanceof Column) {
      Column oc = (Column) o;
      
      return family.equals(oc.family) && qualifier.equals(oc.qualifier) && visibility.equals(oc.visibility);
    }
    
    return false;
  }
  
  public Column() {}

  public Column(ByteSequence family, ByteSequence qualifier) {
    this.family = family;
    this.qualifier = qualifier;
  }
  
  public ByteSequence getFamily() {
    return family;
  }
  
  public ByteSequence getQualifier() {
    return qualifier;
  }

  // TODO should not have ColumnVisibility directly in public API... wrap it
  public Column setVisibility(ColumnVisibility cv) {
    this.visibility = cv;
    return this;
  }
  
  public ColumnVisibility getVisibility() {
    return visibility;
  }

  public String toString() {
    return family + " " + qualifier + " " + visibility;
  }

  // TODO remove from public API
  public void write(DataOutput out) throws IOException {
    ByteUtil.write(out, family);
    ByteUtil.write(out, qualifier);

    WritableUtils.writeVInt(out, visibility.getExpression().length);
    out.write(visibility.getExpression());

  }

  public void readFields(DataInput in) throws IOException {
    family = ByteUtil.read(in);
    qualifier = ByteUtil.read(in);

    int len = WritableUtils.readVInt(in);
    byte[] cv = new byte[len];
    in.readFully(cv);

    // TODO use cv cache?
    visibility = new ColumnVisibility(cv);

  }
}
