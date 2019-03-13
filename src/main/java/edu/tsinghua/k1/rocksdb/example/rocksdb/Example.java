package edu.tsinghua.k1.rocksdb.example.rocksdb;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Created by liukun on 19/3/12.
 */
public class Example {

  public static void main(String[] args) throws RocksDBException {

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
    WriteBatch batch = new WriteBatch();
    for (int i = 0; i < 1000; i++) {
      byte[] key = new String("" + i).getBytes();
      batch.put(key,key);
    }

    db.write(new WriteOptions(),batch);
    RocksIterator iterator = db.newIterator();
    iterator.seekToFirst();
    while(iterator.isValid()){
      byte[] key = iterator.key();
      byte[] value = iterator.value();
      System.out.println("key "+new String(key));
      System.out.println("value "+new String(value));
      iterator.next();
    }
    System.out.println("end");
  }
}
