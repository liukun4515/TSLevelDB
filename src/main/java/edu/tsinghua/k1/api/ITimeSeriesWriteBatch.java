package edu.tsinghua.k1.api;

import org.iq80.leveldb.WriteBatch;

/**
 * Created by liukun on 19/2/27.
 */
public interface ITimeSeriesWriteBatch {

  void write(String timeSeries, long timestamp, byte[] value);

  WriteBatch getData();
}
