package org.apache.parquet.column.values.dictionary;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.RequiresFallback;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

import com.lenovo.cortara.common.util.Bytes;

public class SingleCharDictionaryWriter extends ValuesWriter implements RequiresFallback {

  private Map<Character, Short> dictionary = new HashMap<Character, Short>(Short.MAX_VALUE);
  private Map<Short, Character> indexToDict = new HashMap<Short, Character>(Short.MAX_VALUE);
  private short count = 0;
  private static final int MAX_COUNT = 2 << 14;
  private ByteArrayOutputStream buf;

  private boolean shouldFallBack = false;

  /* encoding to label the data page */
  private final Encoding encodingForDataPage;

  /* encoding to label the dictionary page */
  protected final Encoding encodingForDictionaryPage;

  /*
   * maximum size in bytes allowed for the dictionary will fail over to plain
   * encoding if reached
   */
  protected final int maxDictionaryByteSize;

  public SingleCharDictionaryWriter(int maxDictionaryByteSize, Encoding encodingForDataPage, Encoding encodingForDictionaryPage) {
    this.maxDictionaryByteSize = maxDictionaryByteSize;
    this.encodingForDataPage = encodingForDataPage;
    this.encodingForDictionaryPage = encodingForDictionaryPage;
    buf = new ByteArrayOutputStream(maxDictionaryByteSize);
  }

  @Override
  public boolean shouldFallBack() {
    // TODO check it
    return shouldFallBack;
  }

  @Override
  public boolean isCompressionSatisfying(long rawSize, long encodedSize) {
    // TODO Make sure
    return true;
  }

  @Override
  public void fallBackAllValuesTo(ValuesWriter writer) {
    // TODO Write to other writer

  }

  @Override
  public long getBufferedSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public BytesInput getBytes() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Encoding getEncoding() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void reset() {
    // TODO Auto-generated method stub

  }

  @Override
  public long getAllocatedSize() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String memUsageString(String prefix) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void writeBytes(Binary v) {
    char[] chars = DictionaryUtils.bytes2Chars(v.getBytes());
    byte[] result = new byte[chars.length * 2];
    for (int i = 0; i < chars.length; i++) {
      Character dc = chars[i];
      Short idx = dictionary.get(dc);
      if (idx == null) {
        dictionary.put(dc, count);
        indexToDict.put(count, dc);
        idx = count++;
      }
      checkOverFlow(idx);
      System.arraycopy(DictionaryUtils.short2Bytes(idx), 0, result, i * 2, 2);
    }
    result[0] = markBeginFlag(result[0]);
    
    buf.write(result);
  }

  private void checkOverFlow(int a) {
    if (a > MAX_COUNT) {
      this.shouldFallBack = true;
    }
  }
  
  private byte markBeginFlag(byte b) {
    return (byte) (b | 0x80);
  }
}
