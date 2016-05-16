package org.apache.parquet.column.values.dictionary;

import java.util.Random;

public class TestUtils {
  
  public static String genChinese(Random rand, int len) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < len; i++) {
      int r = rand.nextInt((0x9FFF - 0x4E00) + 1) + 0x4E00;
      str.append((char) r);
    }

    return str.toString();
  }
  
  
  public static int randInt(Random rand, int start,int end) {
      return rand.nextInt((end - start) + 1) + start;
  }
  
  public static String genStr(Random rand, int len) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < len; i++) {
      int r = rand.nextInt(('z' - 'a') + 1) + 'a';
      str.append((char) r);
    }

    return str.toString();
  }
  
  public static boolean equalsBytes(byte[] a , byte[] b) {
    if(a.length == b.length) {
      for(int i = 0 ; i < a.length; i ++) {
        if((a[i] & b[i]) != a[i]) {
          return false;
        }
      }
    } else {
      return false;
    }
    return true;
  }
  
  public static void printByteArray(byte[] bytes) {
    for(byte b : bytes) {
      System.out.print(Integer.toBinaryString((short)b) + " ");
    }
    System.out.println();
  }

  public static void main(String[] args) {
    // TODO Auto-generated method stub

  }

}
