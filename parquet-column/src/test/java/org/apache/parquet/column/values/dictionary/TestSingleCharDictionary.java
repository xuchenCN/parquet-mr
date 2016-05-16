package org.apache.parquet.column.values.dictionary;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.io.api.Binary;
import org.junit.Test;

public class TestSingleCharDictionary {

  @Test
  public void testBinaryWrite() throws IOException {
    int valueCount = 10;
    int termLen = 3;
    Random r = new Random();
    SingleCharDictionaryWriter valueWriter = new SingleCharDictionaryWriter(1024, RLE_DICTIONARY, PLAIN);
    for (int i = 0; i < valueCount; i++) {
      Binary v = Binary.fromString(TestUtils.genChinese(r, termLen));
      valueWriter.writeBytes(v);
    }

    BytesInput bi = valueWriter.getBytes();
    Assert.assertEquals(bi.toByteArray().length, valueCount * termLen * 2, valueWriter.getBufferedSize());
  }
}
