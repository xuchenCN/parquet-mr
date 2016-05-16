package org.apache.parquet.column.values.dictionary;


public class DictChar implements Comparable<DictChar> {

  private char c;

  public DictChar(char c) {
    this.c = c;
  }


  public char getChar() {
    return this.c;
  }

  @Override
  public int compareTo(DictChar o) {
    return this.c - o.c;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + c;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    DictChar other = (DictChar) obj;
    if (c != other.c)
      return false;
    return true;
  }

}
