package edu.tsinghua.k1.rocksdb.example;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Created by liukun on 19/3/12.
 */
public class WriteAndQueryRocksDB {

  static {
    RocksDB.loadLibrary();
  }

  static int cache_number = 10000;
  static int loop = 100;
  static int querystep = 100;
  static int queryCount = 10;

  public static void main(String[] args) {

    File file = new File("timeseries-leveldb-example");
    // this is rocksdb options
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setWriteBufferSize(10<<20);
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
      timeSeriesDB = RocksDBTimeSeriesDBFactory.getInstance().openOrCreate(file,options);
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
