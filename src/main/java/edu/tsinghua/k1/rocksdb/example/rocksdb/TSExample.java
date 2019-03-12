package edu.tsinghua.k1.rocksdb.example.rocksdb;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import edu.tsinghua.k1.rocksdb.RocksDBTimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.rocksdb.Options;

/**
 * Created by liukun on 19/3/12.
 */
public class TSExample {

  public static void main(String[] args) {
    String timeseries = "root.d1.s0";
    Options options = new Options();
    options.setCreateIfMissing(true);
    try {
      ITimeSeriesDB db = RocksDBTimeSeriesDBFactory.getInstance()
          .openOrCreate(new File("hello"), options);
      ITimeSeriesWriteBatch batch = db.createBatch();
      for (int i = 0; i < 100; i++) {
        batch.write(timeseries, i, new byte[10]);
      }
      db.write(batch);
      System.out.println("end of writing data");
      TimeSeriesDBIterator iterator = db.iterator(timeseries, 0, Long.MAX_VALUE);
      while (iterator.hasNext()) {
        byte[] key = iterator.next().getKey();
        byte[] value = iterator.next().getValue();
        System.out.println("get value");
      }
      db.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
