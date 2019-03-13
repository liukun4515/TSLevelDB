package edu.tsinghua.k1.rocksdb.example.rocksdb;

import java.util.concurrent.CountDownLatch;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Created by liukun on 19/3/12.
 */
public class MultiThreadWriteRocksDB {

  private static class Worker implements Runnable {

    private RocksDB db;
    private int id;
    private CountDownLatch latch;

    Worker(RocksDB db, int id, CountDownLatch latch) {
      this.db = db;
      this.id = id;
      this.latch = latch;
    }

    @Override
    public void run() {
      try {
        int count = 0;
        WriteBatch batch = new WriteBatch();
        for (int i = id * 10000000; i < (id + 1) * 100000; i++) {
          try {
            batch.put(new String(i + "").getBytes(), new String(i + "").getBytes());
          } catch (RocksDBException e) {
            e.printStackTrace();
          }
          if (count % 1000 == 0) {
            try {
              this.db.write(new WriteOptions(), batch);
            } catch (RocksDBException e) {
              e.printStackTrace();
            }
            batch = new WriteBatch();
          }
        }
      } finally {
        latch.countDown();
      }
    }
  }

  static int client_num = 10;

  public static void main(String[] args) {
    RocksDB.loadLibrary();

    String path = "test";
    Options options = new Options();
    options.setCreateIfMissing(true);
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
    while (iterator.isValid()) {
      byte[] key = iterator.key();
      System.out.println(new String(key));
      iterator.next();
    }
    System.out.println("end mul-query and write");
  }

}
