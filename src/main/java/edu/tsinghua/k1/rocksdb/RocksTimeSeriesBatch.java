package edu.tsinghua.k1.rocksdb;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.TimeSeriesMap;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBException;
import org.rocksdb.RocksDBException;

/**
 * Created by liukun on 19/3/12.
 */
public class RocksTimeSeriesBatch implements ITimeSeriesWriteBatch {

  private org.rocksdb.WriteBatch batch;

  public RocksTimeSeriesBatch(org.rocksdb.WriteBatch batch) {
    this.batch = batch;
  }

  @Override
  public void write(String timeSeries, long timestamp, byte[] value) throws TimeSeriesDBException {
    byte[] key = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeSeries), timestamp);
    try {
      this.batch.put(key, value);
    } catch (RocksDBException e) {
      e.printStackTrace();
      throw new TimeSeriesDBException(e);
    }
  }

  @Override
  public org.rocksdb.WriteBatch getData() {
    return batch;
  }
}
