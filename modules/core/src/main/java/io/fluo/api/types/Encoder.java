package io.fluo.api.types;

import io.fluo.api.Bytes;

public interface Encoder {
  public int decodeInteger(Bytes b);

  public Bytes encode(int i);

  public long decodeLong(Bytes b);

  public Bytes encode(long l);

  public String decodeString(Bytes b);

  public Bytes encode(String s);
}
