package edu.tsinghua.k1.rocksdb.example.rocksdb;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/**
 * Created by liukun on 19/3/13.
 */
public class MultiThreadTSDBWrite {

  private static class Worker implements Runnable {

    private ITimeSeriesDB db;
    private int id;
    private CountDownLatch latch;
    private String device;

    Worker(ITimeSeriesDB db, int id, CountDownLatch latch) {
      this.db = db;
      this.id = id;
      this.latch = latch;
      this.device = "root.perform." + "d" + id;
    }

    @Override
    public void run() {
      System.out.println("write data begin client: " + id);
      try {
        for (int i = 0; i < loop; i++) {
          ITimeSeriesWriteBatch batch = db.createBatch();
          System.out.println("client: " + id + ", loop:  " + i);
          for (int j = 0; j < cache_num; j++) {
            long time = System.nanoTime();
            for (int k = 0; j < sensor_num; k++) {
              String timeseries = device + "." + "s" + k;
              batch.write(timeseries, time, new byte[20]);
            }
          }
          this.db.write(batch);
        }
      } finally {
        latch.countDown();
      }
      System.out.println("write data end client: " + id);
    }
  }

  private static int client_num = 5;
  private static int sensor_num = 100;
  private static int cache_num = 100;
  private static int loop = 1000;

  public static void main(String[] args) {
    RocksDB.loadLibrary();
    Options options = new Options();
    options.setCreateIfMissing(true);
    ITimeSeriesDB db = null;
    try {
      db = RocksDBTimeSeriesDBFactory.getInstance().openOrCreate(new File("tsbench"), options);
    } catch (IOException e) {
      e.printStackTrace();
    }
    CountDownLatch latch = new CountDownLatch(client_num);
    for (int i = 0; i < client_num; i++) {
      new Thread(new Worker(db, i, latch)).start();
    }

    try {
      latch.await();
      try {
        db.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


}
