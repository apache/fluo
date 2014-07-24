package io.fluo.api.types;

import io.fluo.api.Bytes;

public class StringEncoder implements Encoder {

  @Override
  public int decodeInteger(Bytes b) {
    return Integer.parseInt(decodeString(b));
  }

  @Override
  public Bytes encode(int i) {
    return encode(Integer.toString(i));
  }

  @Override
  public long decodeLong(Bytes b) {
    return Long.parseLong(decodeString(b));
  }

  @Override
  public Bytes encode(long l) {
    return encode(Long.toString(l));
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
