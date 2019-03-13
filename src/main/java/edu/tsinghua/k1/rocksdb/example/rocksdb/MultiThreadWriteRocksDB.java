package edu.tsinghua.k1.rocksdb.example.rocksdb;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.TimeSeriesMap;
import java.util.concurrent.CountDownLatch;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class MultiThreadWriteRocksDB {

  private static class Worker implements Runnable {

    private RocksDB db;
    private int id;
    private CountDownLatch latch;
    private String device;

    Worker(RocksDB db, int id, CountDownLatch latch) {
      this.db = db;
      this.id = id;
      this.latch = latch;
      this.device = "root.perform." + "d" + id;
    }

    @Override
    public void run() {
      System.out.println("Start writing data, client is is:" + id);
      try {
        for (int i = 0; i < loop; i++) {
          WriteBatch batch = new WriteBatch();
          System.out.println("client: " + id + ", loop:  " + i);
          for (int j = 0; j < cache_num; j++) {
            long time = System.nanoTime();
            for (int k = 0; k < sensor_num; k++) {
              String timeseries = device + "." + "s" + k;
              byte[] key = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeseries), time);
              try {
                batch.put(key, new byte[20]);
              } catch (RocksDBException e) {
                e.printStackTrace();
              }
            }
          }
          try {
            this.db.write(new WriteOptions(), batch);
          } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
          }
        }
      } finally {
        latch.countDown();
      }
      System.out.println("End writing data, client is is:" + id);
    }
  }

  private static int client_num = 1;
  private static int sensor_num = 100;
  private static int cache_num = 100;
  private static int loop = 1000;

  public static void main(String[] args) {
    RocksDB.loadLibrary();

    String path = "tsrocksdb";
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setWriteBufferSize(128<<20);

    RocksDB db = null;
    try {
      db = RocksDB.open(options, path);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
    // write data
    CountDownLatch latch = new CountDownLatch(client_num);
    for (int i = 0; i < client_num; i++) {
      new Thread(new Worker(db, i, latch)).start();
    }
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // query data
    RocksIterator iterator = db.newIterator();
    iterator.seekToFirst();
    int count = 0;
    while (iterator.isValid()) {
      byte[] key = iterator.key();
      count++;
      if (count % 10000 == 0) {
        System.out.println(count);
      }
      iterator.next();
    }
    System.out.println("end mul-query and write");
  }

}
