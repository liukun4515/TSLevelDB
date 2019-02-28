package edu.tsinghua.k1;

/**
 * Created by liukun on 19/2/27.
 */
public class ByteUtils {

  public static byte[] getKey(int uid, long time) {
    byte[] data = new byte[12];
    int index = 0;
    // uid identifier
    // 高字节存储在data的低地址位置
    // 低字节存储在data的高地址位置
    data[index + 3] = (byte) (uid);
    data[index + 2] = (byte) (uid >>> 8);
    data[index + 1] = (byte) (uid >>> 16);
    data[index] = (byte) (uid >>> 24);
    // suffix
    // time的高字节存储在低地址位
    data[index + 11] = (byte) (time);
    data[index + 10] = (byte) (time >>> 8);
    data[index + 9] = (byte) (time >>> 16);
    data[index + 8] = (byte) (time >>> 24);
    data[index + 7] = (byte) (time >>> 32);
    data[index + 6] = (byte) (time >>> 40);
    data[index + 5] = (byte) (time >>> 48);
    data[index + 4] = (byte) (time >>> 56);
    return data;
  }

  public static byte[] getValue(String value) {
    return value.getBytes();
  }
}
