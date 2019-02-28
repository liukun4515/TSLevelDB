package edu.tsinghua.k1.api;

/**
 * Created by liukun on 19/2/28.
 */
public interface ISeriesIndex {

  int getSeriesId(String timeseries);

  byte[] getBytesArrayId(String timeseries);
}
