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
