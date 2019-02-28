package edu.tsinghua.k1.api;

import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.Options;

/**
 * Created by liukun on 19/2/27.
 */
public interface ITimeSeriesDBFactory {

  ITimeSeriesDB openOrCreate(File path, Options options) throws IOException;

  void destroy(File path, Options options) throws IOException;

}
