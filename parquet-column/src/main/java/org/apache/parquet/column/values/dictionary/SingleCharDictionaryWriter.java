package org.apache.parquet.column.values.dictionary;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.parquet.Log;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.RequiresFallback;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.api.Binary;

public class SingleCharDictionaryWriter extends ValuesWriter implements RequiresFallback {

  private static final Log LOG = Log.getLog(SingleCharDictionaryWriter.class);

  private Map<Character, Short> dictionary = new HashMap<Character, Short>(Short.MAX_VALUE);
  private Map<Short, Character> indexToDict = new HashMap<Short, Character>(Short.MAX_VALUE);
  private short current = 0;
  private static final int MAX_COUNT = 2 << 14;
  private ByteArrayOutputStream buf;
  private List<Binary> backup = new LinkedList<Binary>();

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
    Iterator<Binary> it = backup.iterator();
    while(it.hasNext()) {
      writer.writeBytes(it.next());
    }
  }

  @Override
  public long getBufferedSize() {
    return buf.size();
  }

  @Override
  public BytesInput getBytes() {
    return BytesInput.from(buf.toByteArray());
  }

  @Override
  public Encoding getEncoding() {
    return encodingForDataPage;
  }

  @Override
  public void reset() {
    buf.reset();
    dictionary.clear();
    indexToDict.clear();
    backup.clear();
    current = 0;
  }

  @Override
  public long getAllocatedSize() {
    return this.maxDictionaryByteSize;
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format(
        "%s SingleCharDictionaryWriter{\n"
          + "%s\n"
          + "%s\n"
        + "%s}\n",
        prefix,
        prefix + " dict:" + dictionary.size() * 2,
        prefix + " values:" + String.valueOf(getBufferedSize()),
        prefix
        );
  }

  @Override
  public void writeBytes(Binary v) {
    backup.add(v);
    char[] chars = DictionaryUtils.bytes2Chars(v.getBytes());
    byte[] result = new byte[chars.length * 2];
    for (int i = 0; i < chars.length; i++) {
      Character dc = chars[i];
      Short idx = dictionary.get(dc);
      if (idx == null) {
        dictionary.put(dc, current);
//        indexToDict.put(current, dc);
        idx = current++;
      }
      //TODO make better infomation
      checkOverFlow(idx);
      System.arraycopy(DictionaryUtils.short2Bytes(idx), 0, result, i * 2, 2);
    }
    result[0] = markBeginFlag(result[0]);

    try {
      buf.write(result);
    } catch (IOException e) {
      LOG.warn("Buffer Error use fallback writer", e);
      shouldFallBack = true;
    }
  }

  private void checkOverFlow(int a) {
    if (a > MAX_COUNT) {
      LOG.warn("Exceed bound use fallback writer value : " + a);
      this.shouldFallBack = true;
    }
  }

  private byte markBeginFlag(byte b) {
    return (byte) (b | 0x80);
  }
}
