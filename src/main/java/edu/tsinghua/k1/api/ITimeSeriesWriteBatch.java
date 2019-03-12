package edu.tsinghua.k1.api;


/**
 * Created by liukun on 19/2/27.
 */
public interface ITimeSeriesWriteBatch {

  void write(String timeSeries, long timestamp, byte[] value);

  Object getData();
}
