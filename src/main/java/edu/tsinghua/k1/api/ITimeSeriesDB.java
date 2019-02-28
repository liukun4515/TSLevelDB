package edu.tsinghua.k1.api;

import java.io.Closeable;

public interface ITimeSeriesDB extends Closeable {

  ITimeSeriesWriteBatch createBatch();

  void write(ITimeSeriesWriteBatch batch) throws TimeSeriesDBException;

  /**
   * Get a TimeSeriesDBIterator with the query condition.
   * @param timeSeries
   * @param startTime the start time of the query
   * @param endTime the end time of the query
   * @return a TimeSeriesDBIterator with data.
   * @throws TimeSeriesDBException
   */
  TimeSeriesDBIterator iterator(String timeSeries, long startTime,long endTime ) throws TimeSeriesDBException;
}
