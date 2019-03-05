package edu.tsinghua.k1;

/**
 * Created by liukun on 19/2/28.
 */
public class Main {

  public static void main(String[] args) {
      long value = 124;
      System.out.println(ByteUtils.getTime(ByteUtils.getTimeBytes(value)));
  }

}
