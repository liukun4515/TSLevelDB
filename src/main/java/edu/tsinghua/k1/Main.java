package edu.tsinghua.k1;

/**
 * Created by liukun on 19/2/28.
 */
public class Main {

  public static void main(String[] args) {
    int value = Integer.MAX_VALUE;
    byte[] bytes = ByteUtils.int32TOBytes(value);
    System.out.println(ByteUtils.bytesTOInt32(bytes));
    byte b = -1;

    System.out.println(b);
    System.out.println(0xFF);
  }

}
