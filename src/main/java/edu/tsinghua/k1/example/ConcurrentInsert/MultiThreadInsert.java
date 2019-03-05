package edu.tsinghua.k1.example.ConcurrentInsert;


import edu.tsinghua.k1.BaseTimeSeriesDBFactory;
import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.TimeSeriesMap;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.iq80.leveldb.Options;

/**
 * 实现：
 * 1. 多客户端
 * 2. 多设备
 * 3. 多sensor
 * 4. 多num cache
 * 5. 多loop
 * 的插入行测试。
 *
 * 检测多线程写入是否出现数据丢失
 */
public class MultiThreadInsert {

  static int num_client = 5;
  static int device_num = 10;
  static int sensor_num = 10;
  static int cache_number = 100;
  static int loop = 10;

  static long startTime = 0;
  static int querystep = 100;
  static int queryCount = 10;

  static List<List<String>> generateTimeSeries(int device_size, int sensor_size) {
    List<List<String>> timeseries = new ArrayList<>();
    for (int i = 0; i < device_size; i++) {
      List<String> temp = new ArrayList<>();
      for (int j = 0; j < sensor_size; j++) {
        temp.add("d" + i + "s" + j);
      }
      timeseries.add(temp);
    }
    return timeseries;
  }


  public static int queryData(ITimeSeriesDB db, String timeseries, long start, long end) {
    TimeSeriesDBIterator iterator = db.iterator(timeseries, start, end);
    int count = 0;
    while (iterator.hasNext()) {
      count++;
      iterator.next();
    }
    return count;
  }

  public static class Worker implements Runnable {

    private int loop;
    private CountDownLatch latch;
    private long startTime;
    private List<List<String>> deviceAndSensor;
    private ITimeSeriesDB db;

    public Worker(int loop, CountDownLatch latch, long startTime,
        List<List<String>> deviceAndSensor, ITimeSeriesDB db) {
      this.loop = loop;
      this.latch = latch;
      this.startTime = startTime;
      this.deviceAndSensor = deviceAndSensor;
      this.db = db;
    }

    @Override
    public void run() {
      System.out.println(Thread.currentThread().getName() + " start write data");
      try {
        byte[] value = ByteUtils.int32TOBytes(20);
        for (int i = 0; i < loop; i++) {
          for (int j = 0; j < deviceAndSensor.size(); j++) {
            List<String> sensors = deviceAndSensor.get(j);
            // 一个设备的sensor
            ITimeSeriesWriteBatch batch = db.createBatch();
            // 一个设备的cache num 行数据
            // 数据点为 cache num* sensor
            for (long k = startTime; k < startTime + cache_number; k++) {
              // 一个设备的一行数据
              for (String sensor : sensors) {
                batch.write(sensor, k, value);
              }
            }
            db.write(batch);
          }
          startTime += cache_number;
        }
      } finally {
        this.latch.countDown();
      }
      System.out.println(Thread.currentThread().getName() + " end write data");
    }
  }

  public static void insertData(List<List<String>> ds, ITimeSeriesDB timeSeriesDB) {
    CountDownLatch latch = new CountDownLatch(num_client);
    for (int i = 0; i < num_client; i++) {
      Thread thread = new Thread(new Worker(loop, latch, startTime, ds, timeSeriesDB));
      thread.setName("work-" + i);
      thread.start();
    }
    try {
      latch.await();
      System.out.println("Insert all data over");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void queryTimeSeries(List<List<String>> ds) {
    for (List<String> times : ds) {
      for (String s : times) {
        System.out.println("query ds is " + s + " id:" + TimeSeriesMap.getInstance().getUid(s));
      }
    }
  }

  public static void queryData(List<List<String>> ds, ITimeSeriesDB timeSeriesDB) {
    for (List<String> series : ds) {
      for (String s : series) {
        int count = queryData(timeSeriesDB, s, 0, Long.MAX_VALUE);
        System.out.println("timeseires " + s + " time " + 0 + " " + -1 + " count " + count);
        if (s.equals("d1s1")) {
          TimeSeriesDBIterator iterator = timeSeriesDB.iterator(s, 0, cache_number * (loop + 1));
          int v = 0;
          while (iterator.hasNext()) {
            v++;
            iterator.next();
          }
          System.out.println("vvvvv" + v);
        }
//        for (int i = 0; i < loop; i++) {
//          long start = cache_number * i;
//          long end = cache_number * i + cache_number;
//          int count = queryData(timeSeriesDB, s, start, end);
//          System.out.println("timeseires " + s + "time " + start + " " + end + "count " + count);
//        }
      }
    }
  }

  public static void main(String[] args) {
    File file = new File("timeseries-leveldb-example");

    Options options = new Options();
    options.createIfMissing(true);
    options.writeBufferSize(10 << 20);
    // 根据需求配置options
    // 创建time series db
    ITimeSeriesDB timeSeriesDB = null;
    try {
      timeSeriesDB = BaseTimeSeriesDBFactory.getInstance().openOrCreate(file, options);
      List<List<String>> ds = generateTimeSeries(device_num, sensor_num);
      // create worker
//      insertData(ds, timeSeriesDB);
      // query result
      queryTimeSeries(ds);
//      queryData(ds, timeSeriesDB);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      try {
        if (timeSeriesDB != null) {
          timeSeriesDB.close();
        }
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }
}
