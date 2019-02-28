package edu.tsinghua.k1.example;

import edu.tsinghua.k1.BaseTimeSeriesDBFactory;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.iq80.leveldb.Options;

/**
 * Created by liukun on 19/2/28.
 */
public class ExampleForTimeSeriesDB {


  public static void main(String[] args) throws IOException {
    File file = new File("timeseries-leveldb-example");
    Options options = new Options();
    options.createIfMissing(true);
    // 根据需求配置options
    // 创建time series db
    ITimeSeriesDB timeSeriesDB = null;
    try {
      timeSeriesDB = BaseTimeSeriesDBFactory.getInstance().openOrCreate(file, options);
      // write data
      // create batch
      ITimeSeriesWriteBatch batch = timeSeriesDB.createBatch();
      // write data to batch
      String timeseries = "root.g1.s1";
      long timestamp = 1111111;
      byte[] bytes = "hello world".getBytes();
      batch.write(timeseries,timestamp,bytes);
      // write batch to DB
      timeSeriesDB.write(batch);

      // query data with range which contains data
      TimeSeriesDBIterator dbIterator = timeSeriesDB.iterator(timeseries,0,1111111);
      while(dbIterator.hasNext()){
        Map.Entry<byte[],byte[]> entry = dbIterator.next();
        String value = new String(entry.getValue());
        System.out.println(value);
      }

      // query data with range which does not contain data
      dbIterator = timeSeriesDB.iterator(timeseries,0,1111110);
      while(dbIterator.hasNext()){
        Map.Entry<byte[],byte[]> entry = dbIterator.next();
        String value = new String(entry.getValue());
        System.out.println(value);
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if(timeSeriesDB!=null){
        timeSeriesDB.close();
      }
    }

    // destroy
    BaseTimeSeriesDBFactory.getInstance().destroy(file,options);
  }

}
