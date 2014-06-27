package org.fluo.api.types;

import org.apache.accumulo.core.data.ByteSequence;

public interface Encoder {
  public int decodeInteger(ByteSequence bs);

  public ByteSequence encode(int i);

  public long decodeLong(ByteSequence bs);

  public ByteSequence encode(long l);

  public String decodeString(ByteSequence bs);

  public ByteSequence encode(String s);
}
