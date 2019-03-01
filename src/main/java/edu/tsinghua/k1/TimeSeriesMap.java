package edu.tsinghua.k1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
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

  public int getUid(String timeSeries) {
    if (!timeSeriesToUID.contains(timeSeries)) {
      int uid = UIDAllocator.getInstance().getId();
      timeSeriesToUID.putIfAbsent(timeSeries, uid);
    }
    return timeSeriesToUID.get(timeSeries);
  }

  public void serialize(File file) throws IOException {
    try (RandomAccessFile reader = new RandomAccessFile(file, "rw")) {
      reader.seek(4);
      reader.writeInt(timeSeriesToUID.size());
      for (Map.Entry<String, Integer> entry : timeSeriesToUID.entrySet()) {
        reader.writeUTF(entry.getKey());
        reader.writeInt(entry.getValue());
      }
    }
  }

  public void deserialize(File file) {
    try (RandomAccessFile reader = new RandomAccessFile(file, "rw")) {
      if (reader.length() >= 8) {
        // skip id
        reader.seek(4);
        int size = reader.readInt();
        timeSeriesToUID = new ConcurrentHashMap<>();
        for (int i = 0; i < size; i++) {
          String key = reader.readUTF();
          int value = reader.readInt();
          timeSeriesToUID.put(key, value);
        }
      }
    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
