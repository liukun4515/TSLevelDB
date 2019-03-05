package edu.tsinghua.k1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liukun on 19/2/27.
 */
public class UIDAllocator {

  private AtomicInteger idGenerator;

  private UIDAllocator() {
    // deserialize
    idGenerator = new AtomicInteger(0);
  }

  public int getId() {
    int value = idGenerator.getAndIncrement();
    return value;
  }

  private static class InstanceHolder {

    private static UIDAllocator instance = new UIDAllocator();
  }

  public static UIDAllocator getInstance() {
    return InstanceHolder.instance;
  }

  public void serialize(File file) throws IOException {
    try (RandomAccessFile reader = new RandomAccessFile(file, "rw")) {
      int nextID = idGenerator.getAndIncrement();
      reader.writeInt(nextID);
    }
  }

  public void deserialize(File file) {
    int id = 0;
    try (RandomAccessFile reader = new RandomAccessFile(file, "rw");) {
      if (reader.length() >= 4) {
        id = reader.readInt();
        System.out.println("recovered id"+ id);
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
    idGenerator = new AtomicInteger(id);
  }
}
