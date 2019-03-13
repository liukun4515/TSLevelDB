package edu.tsinghua.k1.rocksdb;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Created by liukun on 19/3/12.
 */
public class RocksDBTimeSeriesDBFactory implements ITimeSeriesDBFactory {

  static {
    RocksDB.loadLibrary();
  }

  private static class Holder{
    private static RocksDBTimeSeriesDBFactory instance = new RocksDBTimeSeriesDBFactory();
  }

  public static RocksDBTimeSeriesDBFactory getInstance(){
    return  Holder.instance;
  }

  @Override
  public ITimeSeriesDB openOrCreate(File path, Object options) throws IOException {
    ITimeSeriesDB db;
    try {
      RocksDB rocksDB = RocksDB.open((org.rocksdb.Options) options, path.getPath());
      db = new RocksDBTimeSeriesDB(path, rocksDB);
    } catch (RocksDBException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
    return db;
  }

  @Override
  public void destroy(File path, Object options) throws IOException {
    try {
      RocksDB.destroyDB(path.getPath(), (Options) options);
      ((Options) options).close();
    } catch (RocksDBException e) {
      e.printStackTrace();
      throw new IOException(e);
    }
  }
}
