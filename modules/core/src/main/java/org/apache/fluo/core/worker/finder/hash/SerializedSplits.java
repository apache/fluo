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

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Bytes.BytesBuilder;
import org.apache.fluo.core.impl.Environment;
import org.apache.fluo.core.util.ByteUtil;

public class SerializedSplits {

  static final int MAX_SIZE = 1 << 18;

  public static void deserialize(Consumer<Bytes> splitConsumer, byte[] serializedSplits) {
    try {
      ByteArrayInputStream bais = new ByteArrayInputStream(serializedSplits);
      GZIPInputStream gzis = new GZIPInputStream(bais);
      DataInputStream dis = new DataInputStream(gzis);

      int numSplits = dis.readInt();

      BytesBuilder builder = Bytes.builder();

      for (int i = 0; i < numSplits; i++) {
        int len = dis.readInt();
        builder.setLength(0);
        builder.append(dis, len);
        splitConsumer.accept(builder.toBytes());
      }

    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static byte[] serializeInternal(List<Bytes> splits) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    GZIPOutputStream gzOut = new GZIPOutputStream(baos);
    BufferedOutputStream bos = new BufferedOutputStream(gzOut, 1 << 16);
    DataOutputStream dos = new DataOutputStream(bos);

    dos.writeInt(splits.size());
    for (Bytes split : splits) {
      dos.writeInt(split.length());
      split.writeTo(dos);
    }

    dos.close();

    return baos.toByteArray();
  }

  public static byte[] serialize(Collection<Bytes> splits) {
    List<Bytes> splitsCopy = new ArrayList<>(splits);
    Collections.sort(splitsCopy);

    try {
      byte[] serialized = serializeInternal(splitsCopy);

      while (serialized.length > MAX_SIZE) {
        List<Bytes> splitsCopy2 = new ArrayList<>(splitsCopy.size() / 2 + 1);
        for (int i = 0; i < splitsCopy.size(); i += 2) {
          splitsCopy2.add(splitsCopy.get(i));
        }

        splitsCopy = splitsCopy2;
        serialized = serializeInternal(splitsCopy);
      }

      return serialized;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static byte[] serializeTableSplits(Environment env) {
    List<Bytes> splits;
    try {
      splits = env.getConnector().tableOperations().listSplits(env.getTable()).stream()
          .map(ByteUtil::toBytes).collect(Collectors.toList());
    } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
      throw new RuntimeException(e);
    }
    return serialize(splits);
  }
}
