package edu.tsinghua.k1.api;

/**
 * Created by liukun on 19/2/27.
 */
public class TimeSeriesDBException extends RuntimeException {

  public TimeSeriesDBException() {
  }

  public TimeSeriesDBException(String message) {
    super(message);
  }

  public TimeSeriesDBException(String message, Throwable cause) {
    super(message, cause);
  }

  public TimeSeriesDBException(Throwable cause) {
    super(cause);
  }
}
