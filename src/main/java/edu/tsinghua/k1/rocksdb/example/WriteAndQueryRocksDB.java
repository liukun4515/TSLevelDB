package edu.tsinghua.k1.rocksdb.example;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.rocksdb.DBOptions;
import org.rocksdb.HistogramType;
import org.rocksdb.InfoLogLevel;
import org.rocksdb.Logger;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.Statistics;

/**
 * Created by liukun on 19/3/12.
 */
public class WriteAndQueryRocksDB {

  static {
    RocksDB.loadLibrary();
  }

  static int cache_number = 10000;
  static int loop = 10000;
  static int querystep = 100;
  static int queryCount = 10;


  static class MyLogger extends Logger {

    public MyLogger(Options options) {
      super(options);
    }

    public MyLogger(DBOptions dboptions) {
      super(dboptions);
    }

    @Override
    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
      System.out.println("rocksdb logger: " + logMsg);
    }
  }

  public static void main(String[] args) {

    File file = new File("timeseries-leveldb-example");
    // this is rocksdb options
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setWriteBufferSize(32 << 20);
    options.setLogger(new MyLogger(options));
    options.setStatsDumpPeriodSec(20);
    options.setStatistics(new Statistics());
    // 根据需求配置options
    // 创建time series db
    ITimeSeriesDB timeSeriesDB = null;
    String timeseries = "root.g0.d0.s0";
    long startTime = 0;
    long time = startTime;
    long endTime = 0;
    long step = 1;
    byte[] value = ByteUtils.int32TOBytes(10);
    try {
      timeSeriesDB = RocksDBTimeSeriesDBFactory.getInstance().openOrCreate(file, options);
      // insert data
      for (int i = 0; i < loop; i++) {
        ITimeSeriesWriteBatch batch = timeSeriesDB.createBatch();
        for (int j = 0; j < cache_number; j++) {
          batch.write(timeseries, time, value);
          time += step;
        }
        timeSeriesDB.write(batch);
        System.out.println("loop number" + i);
      }
      endTime = time;
      // query
      long randomTime = startTime;
      for (int i = 0; i < queryCount; i++) {
        randomTime = randomTime++;
        long randomEndTime = randomTime + querystep - 1;
        TimeSeriesDBIterator iterator = timeSeriesDB
            .iterator(timeseries, randomTime, randomEndTime);
        int count = 0;
        while (iterator.hasNext()) {
          iterator.next();
          count++;
        }
        System.out.println("query value = " + count);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        timeSeriesDB.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    try {
      RocksDBTimeSeriesDBFactory.getInstance().destroy(file,options);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
