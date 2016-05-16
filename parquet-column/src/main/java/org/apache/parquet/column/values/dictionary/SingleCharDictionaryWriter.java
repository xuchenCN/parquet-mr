package org.apache.parquet.column.values.dictionary;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.RequiresFallback;
import org.apache.parquet.column.values.ValuesWriter;

public class SingleCharDictionaryWriter extends ValuesWriter implements RequiresFallback {
  
  private boolean shouldFallBack = false;

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
  
  
}
