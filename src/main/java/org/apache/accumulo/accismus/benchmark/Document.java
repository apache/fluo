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
package org.apache.accumulo.accismus.benchmark;

import java.util.Random;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.FastFormat;

/**
 * 
 */
public class Document {
  
  private static final byte[] URL_PREFIX = "url:".getBytes();
  private static final byte[] K1_PREFIX = "ke1:".getBytes();
  private static final byte[] K2_PREFIX = "ke2:".getBytes();
  private static final byte[] K3_PREFIX = "ke3:".getBytes();

  ByteSequence uri;
  ByteSequence content;
  
  public Document(Random rand) {
    uri = new ArrayByteSequence(FastFormat.toZeroPaddedString(rand.nextInt(1000000000), 8, 16, URL_PREFIX));
    content = new ArrayByteSequence(new byte[1000]);
    rand.nextBytes(content.getBackingArray());
  }

  public Document(ByteSequence uri, ByteSequence doc) {
    this.uri = uri;
    this.content = doc;
  }

  public ByteSequence getUrl() {
    return uri;
  }
  
  private ByteSequence createKey(ByteSequence seq, byte[] prefix) {
    int k = (((0xff & seq.byteAt(0)) << 24) + ((0xff & seq.byteAt(1)) << 16) + ((0xff & seq.byteAt(2)) << 8) + ((0xff & seq.byteAt(3)) << 0));
    k = Math.abs(k) % 750000000;
    
    return new ArrayByteSequence(FastFormat.toZeroPaddedString(k, 8, 16, prefix));
  }
  
  public ByteSequence getKey1() {
    return createKey(content.subSequence(0, 4), K1_PREFIX);
  }

  public ByteSequence getKey2() {
    return createKey(content.subSequence(4, 8), K2_PREFIX);
  }
  
  public ByteSequence getKey3() {
    return createKey(content.subSequence(8, 12), K3_PREFIX);
  }
  
  public ByteSequence getContent() {
    return content;
  }

}
