package edu.tsinghua.k1;

import com.sun.xml.internal.messaging.saaj.util.ByteInputStream;
import com.sun.xml.internal.messaging.saaj.util.ByteOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by liukun on 19/2/27.
 */
public class TimeSeriesMap {

  private ConcurrentHashMap<String, Integer> timeSeriesToUID;

  private TimeSeriesMap() {
    timeSeriesToUID = new ConcurrentHashMap<>();
  }

  private static class InstanceHolder {

    private static TimeSeriesMap instance = new TimeSeriesMap();
  }

  public static TimeSeriesMap getInstance() {
    return InstanceHolder.instance;
  }

  public synchronized int getUid(String timeSeries) {
    if (!timeSeriesToUID.containsKey(timeSeries)) {
      int uid = UIDAllocator.getInstance().getId();
      timeSeriesToUID.putIfAbsent(timeSeries, uid);
    }
    return timeSeriesToUID.get(timeSeries);
  }

  public void serialize(File file) throws IOException {
    try (RandomAccessFile reader = new RandomAccessFile(file, "rw")) {
      reader.seek(4);
      ByteOutputStream outputStream = new ByteOutputStream();
      ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
      objectOutputStream.writeObject(this.timeSeriesToUID);
      for (Map.Entry<String, Integer> entry : this.timeSeriesToUID.entrySet()) {
        System.out.println("ser2.0 key value: " + entry.getKey() + " " + entry.getValue());
      }
      reader.write(outputStream.getBytes());
    }
  }

  public void deserialize(File file) {
    try (RandomAccessFile reader = new RandomAccessFile(file, "rw")) {
      if (reader.length() >= 8) {
        // skip id
        reader.seek(4);
        int size = reader.readInt();
        byte[] bytes = new byte[(int) (reader.length() - 4)];
        reader.readFully(bytes);
        ByteInputStream inputStream = new ByteInputStream(bytes, bytes.length);
        ObjectInputStream ObjectInputStream = new ObjectInputStream(inputStream);
        this.timeSeriesToUID = (ConcurrentHashMap<String, Integer>) ObjectInputStream.readObject();
        for (Map.Entry<String, Integer> entry : this.timeSeriesToUID.entrySet()) {
          System.out.println("des2.0 key value: " + entry.getKey() + " " + entry.getValue());
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
  }
}
