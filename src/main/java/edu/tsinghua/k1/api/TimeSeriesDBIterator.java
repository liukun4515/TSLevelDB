package edu.tsinghua.k1.api;

import java.util.Map;

public interface TimeSeriesDBIterator {

  /**
   * Return {@code true} if the TimeSeriesDBIterator has more elements.
   * @return {@code true} if the Iterator has more elements
   */
  boolean hasNext();

  /**
   * Return the next element
   * @return the next element in the iteration
   * @throws TimeSeriesDBException if the iteration has no more element
   */
  Map.Entry<byte[],byte[]> next();

  /**
   * Must call this function to release the resources.
   */
  void close();

}
