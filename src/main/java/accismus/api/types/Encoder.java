package accismus.api.types;

import org.apache.accumulo.core.data.ByteSequence;

public interface Encoder {
  public int decodeInteger(ByteSequence bs);

  public ByteSequence encodeInteger(int i);

  public long decodeLong(ByteSequence bs);

  public ByteSequence encodeLong(long l);

  public String decodeString(ByteSequence bs);

  public ByteSequence encodeString(String s);
}
