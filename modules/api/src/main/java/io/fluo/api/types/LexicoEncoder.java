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
package io.fluo.api.types;

import io.fluo.api.data.Bytes;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;

public class LexicoEncoder implements Encoder {

  private IntegerLexicoder il = new IntegerLexicoder();
  private LongLexicoder ll = new LongLexicoder();

  @Override
  public int decodeInteger(Bytes b) {
    return il.decode(b.toArray());
  }

  @Override
  public Bytes encode(int i) {
    return Bytes.wrap(il.encode(i));
  }

  @Override
  public long decodeLong(Bytes b) {
    return ll.decode(b.toArray());
  }

  @Override
  public Bytes encode(long l) {
    return Bytes.wrap(ll.encode(l));
  }

  @Override
  public String decodeString(Bytes b) {
    return b.toString();
  }

  @Override
  public Bytes encode(String s) {
    return Bytes.wrap(s);
  }
}
