package org.fluo.api.types;

import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

public class StringEncoder implements Encoder {

  @Override
  public int decodeInteger(ByteSequence bs) {
    return Integer.parseInt(decodeString(bs));
  }

  @Override
  public ByteSequence encode(int i) {
    return encode(Integer.toString(i));
  }

  @Override
  public long decodeLong(ByteSequence bs) {
    return Long.parseLong(decodeString(bs));
  }

  @Override
  public ByteSequence encode(long l) {
    return encode(Long.toString(l));
  }

  @Override
  public String decodeString(ByteSequence bs) {
    try {
      return new String(bs.toArray(), "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ByteSequence encode(String s) {
    try {
      return new ArrayByteSequence(s.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

}
