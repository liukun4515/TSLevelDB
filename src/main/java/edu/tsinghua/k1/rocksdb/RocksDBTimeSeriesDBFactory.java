package edu.tsinghua.k1.rocksdb;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.rocksdb.DBOptions;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.LRUCache;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;

/**
 * Created by liukun on 19/3/12.
 */
public class RocksDBTimeSeriesDBFactory implements ITimeSeriesDBFactory {

  static {
    RocksDB.loadLibrary();
  }

  private static class MyLogger extends Logger {

    public MyLogger(Options options) {
      super(options);
    }

    public MyLogger(DBOptions dboptions) {
      super(dboptions);
    }

    @Override
    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
      System.out.println("Rocksdb Logger: " + logMsg);
    }
  }

  private static class Holder {

    private static RocksDBTimeSeriesDBFactory instance = new RocksDBTimeSeriesDBFactory();
  }

  public static RocksDBTimeSeriesDBFactory getInstance() {
    return Holder.instance;
  }

  @Override
  public ITimeSeriesDB openOrCreate(File path, Object options) throws IOException {
    ITimeSeriesDB db;
    Options rocksOptions = (Options) options;
    // 128M memtable and l0 sstable
    // distable compaction
//    rocksOptions.setDisableAutoCompactions(true);
    rocksOptions.setWriteBufferSize(128 << 20);
    rocksOptions.setMaxWriteBufferNumber(2);
    rocksOptions.setMaxBackgroundFlushes(1);
    rocksOptions.setBaseBackgroundCompactions(1);
    rocksOptions.setMinWriteBufferNumberToMerge(1);
    rocksOptions.setTargetFileSizeBase(128 << 20);
    rocksOptions.setLevel0FileNumCompactionTrigger(4);
    rocksOptions.setMaxBytesForLevelBase(512 << 20);
    rocksOptions.setMaxOpenFiles(1048576);
    rocksOptions.setReportBgIoStats(true);
    // zero cache size
    rocksOptions.setRowCache(new LRUCache(0));
    rocksOptions.setWalDir("/data2/liukun-data/rocksdb_wal");
    // set stat
    rocksOptions.setStatistics(new Statistics());
    rocksOptions.setStatsDumpPeriodSec(60);
    // set logger
    rocksOptions.setLogger(new MyLogger(rocksOptions));

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
