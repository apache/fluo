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

package org.apache.fluo.core.data;

import org.apache.fluo.accumulo.data.MutableBytes;
import org.apache.fluo.api.data.Bytes;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link MutableBytes}
 */
public class MutableBytesTest {

  @Test
  public void testImmutableBytes() {
    byte[] d1 = Bytes.of("mydata").toArray();

    MutableBytes mutable = new MutableBytes(d1);
    Assert.assertNotSame(d1, mutable.toArray());

    Bytes immutable = Bytes.of(d1);
    Assert.assertNotSame(d1, immutable.toArray());
    Assert.assertEquals(mutable, immutable);
    Assert.assertNotSame(mutable, immutable);

    Bytes read = mutable;
    Assert.assertEquals(read, immutable);
    Assert.assertSame(read, mutable);
    Assert.assertEquals(read, mutable);
    Assert.assertNotSame(d1, read.toArray());

    MutableBytes write = (MutableBytes) immutable;
    Assert.assertEquals(write, mutable);
    Assert.assertNotSame(write, mutable);
    byte[] d2 = write.toArray();
    Assert.assertNotSame(d2, write.toArray());
  }
}
