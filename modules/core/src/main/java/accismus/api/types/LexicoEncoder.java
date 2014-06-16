package accismus.api.types;

import java.io.UnsupportedEncodingException;

import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;

public class LexicoEncoder implements Encoder {

  private IntegerLexicoder il = new IntegerLexicoder();
  private LongLexicoder ll = new LongLexicoder();
  
  @Override
  public int decodeInteger(ByteSequence bs) {
    return il.decode(bs.toArray());
  }

  @Override
  public ByteSequence encode(int i) {
    return new ArrayByteSequence(il.encode(i));
  }

  @Override
  public long decodeLong(ByteSequence bs) {
    return ll.decode(bs.toArray());
  }

  @Override
  public ByteSequence encode(long l) {
    return new ArrayByteSequence(ll.encode(l));
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
