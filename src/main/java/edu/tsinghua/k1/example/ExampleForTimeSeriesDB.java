package edu.tsinghua.k1.example;

import edu.tsinghua.k1.BaseTimeSeriesDBFactory;
import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.iq80.leveldb.Logger;
import org.iq80.leveldb.Options;

/**
 * Created by liukun on 19/2/28.
 */
public class ExampleForTimeSeriesDB {


  public static class Client implements Runnable {

    private ITimeSeriesDB db;
    private String timeSeries;
    private long startTime;
    private CountDownLatch latch;
    private long count = 10000000;
    private long step = 1000;

    public Client(ITimeSeriesDB db, String timeSeries, long startTime,
        CountDownLatch latch) {
      this.db = db;
      this.timeSeries = timeSeries;
      this.startTime = startTime;
      this.latch = latch;
    }

    @Override
    public void run() {
      ITimeSeriesWriteBatch batch = db.createBatch();
      for (int i = 0; i < count; i++) {
        if ((i + 1) % step == 0) {
          db.write(batch);
          batch = db.createBatch();
          System.out.println(Thread.currentThread().getId() + " write data");
        }
        batch.write(timeSeries, startTime + i, ByteUtils.int32TOBytes(100));
      }
      latch.countDown();
    }
  }

  public static void main(String[] args) throws IOException {
    File file = new File("timeseries-leveldb-example");

    Options options = new Options();
    options.createIfMissing(true);
    options.logger(new Logger() {
      @Override
      public void log(String s) {
        System.out.println(s);
      }
    });
    options.writeBufferSize(10 << 20);
    // 根据需求配置options
    // 创建time series db
    ITimeSeriesDB timeSeriesDB = null;
    try {
      timeSeriesDB = BaseTimeSeriesDBFactory.getInstance().openOrCreate(file, options);
      CountDownLatch latch = new CountDownLatch(5);
      String timeseries = "root.g1.s";
      for (int i = 0; i < 5; i++) {
        Thread thread = new Thread(new Client(timeSeriesDB, timeseries + i, i * 10000000, latch));
        System.out.println("Client " + i);
        thread.start();
      }
      // 查询代码
//      TimeSeriesDBIterator dbIterator = timeSeriesDB.iterator(timeseries, 0, 1111111);
//      while (dbIterator.hasNext()) {
//        Map.Entry<byte[], byte[]> entry = dbIterator.next();
//        String value = new String(entry.getValue());
//        System.out.println(value);
//      }
//
//      // query data with range which does not contain data
//      dbIterator = timeSeriesDB.iterator(timeseries, 0, 1111110);
//      while (dbIterator.hasNext()) {
//        Map.Entry<byte[], byte[]> entry = dbIterator.next();
//        String value = new String(entry.getValue());
//        System.out.println(value);
//      }
      latch.await();
    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
      timeSeriesDB.close();
    } finally {
      if (timeSeriesDB != null) {
        timeSeriesDB.close();
      }
    }

    // destroy
    //BaseTimeSeriesDBFactory.getInstance().destroy(file, options);
  }

}
