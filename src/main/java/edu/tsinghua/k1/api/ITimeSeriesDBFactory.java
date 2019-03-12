package edu.tsinghua.k1.api;

import java.io.File;
import java.io.IOException;

/**
 * Created by liukun on 19/2/27.
 */
public interface ITimeSeriesDBFactory {

  ITimeSeriesDB openOrCreate(File path, Object options) throws IOException;

  void destroy(File path, Object options) throws IOException;

}
