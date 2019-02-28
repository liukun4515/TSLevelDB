package edu.tsinghua.k1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liukun on 19/2/27.
 */
public class UIDAllocator {

  private AtomicInteger idGenerator;

  private UIDAllocator() {
    // deserialize
    idGenerator = new AtomicInteger(1);
  }

  public int getId() {
    return idGenerator.getAndIncrement();
  }

  private static class InstanceHolder {

    private static UIDAllocator instance = new UIDAllocator();
  }

  public static UIDAllocator getInstance() {
    return InstanceHolder.instance;
  }

  public void serialize(File file) throws IOException {
    FileOutputStream outputStream = new FileOutputStream(file);
    int nextID = idGenerator.getAndIncrement();
    byte[] data = ByteUtils.int32TOBytes(nextID);
    outputStream.write(data);
    outputStream.close();
  }

  public void deserialize(File file) {
    FileInputStream inputStream = null;
    try {
      inputStream = new FileInputStream(file);
      byte[] data = new byte[4];
      inputStream.read(data);
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

//    int id =
  }
}
