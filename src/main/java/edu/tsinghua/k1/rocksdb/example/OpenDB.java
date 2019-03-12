package edu.tsinghua.k1.rocksdb.example;

import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class OpenDB {


  public static void main(String[] args) {
    RocksDB.loadLibrary();
    Options rockopts = new Options().setCreateIfMissing(false);
    RocksDB db = null;

    try {
      db = RocksDB.open(rockopts, args[0]);
      String query = args[1];
      db.close();
    } catch (RocksDBException rdbe) {
      rdbe.printStackTrace(System.err);
    }
  }

}
