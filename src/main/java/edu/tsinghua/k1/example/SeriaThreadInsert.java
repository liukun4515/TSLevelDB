package edu.tsinghua.k1.example;

import edu.tsinghua.k1.BaseTimeSeriesDBFactory;
import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.Options;

/**
 * 串行数据写入，检查数据写入的争取性
 */
public class SeriaThreadInsert {

  static int cache_number = 10000;
  static int loop = 100;
  static int querystep = 100;
  static int queryCount = 10;

  public static void main(String[] args) {
    File file = new File("timeseries-leveldb-example");
    Options options = new Options();
    options.createIfMissing(true);
    options.writeBufferSize(10 << 20);
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
      timeSeriesDB = BaseTimeSeriesDBFactory.getInstance().openOrCreate(file, options);
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
  }
}
