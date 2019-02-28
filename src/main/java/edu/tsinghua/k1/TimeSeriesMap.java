package edu.tsinghua.k1;

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
}
