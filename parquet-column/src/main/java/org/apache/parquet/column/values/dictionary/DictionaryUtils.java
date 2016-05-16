package org.apache.parquet.column.values.dictionary;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;

import org.apache.parquet.io.api.Binary;

public class DictionaryUtils {
  public static short bytes2Short(byte[] bytes) {
    return (short) (((short) bytes[0] << 8) + ((short) bytes[1] << 0));
  }

  public static byte[] short2Bytes(short c) {
    byte[] b = new byte[2];
    b[0] = (byte) (c >>> 8);
    b[1] = (byte) (c);
    return b;
  }

  public static char[] bytes2Chars(byte[] bytes) {
    Charset cs = Charset.forName("UTF-8");
    ByteBuffer bb = ByteBuffer.allocate(bytes.length);
    bb.put(bytes);
    bb.flip();
    CharBuffer cb = cs.decode(bb);
    char[] caa = new char[bytes.length / 3];
    cb.get(caa);
    return caa;
  }

  public static void main(String[] args) throws UnsupportedEncodingException {

    byte[] b2 = Binary.fromString("好莱坞").getBytes();
    char[] c = bytes2Chars(b2);
  }
}
