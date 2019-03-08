package edu.tsinghua.k1.leveldb;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.TimeSeriesMap;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import org.iq80.leveldb.WriteBatch;

/**
 * Created by liukun on 19/2/27.
 */
public class TimeSeriesBatch implements ITimeSeriesWriteBatch {

  private WriteBatch batch;

  public TimeSeriesBatch(WriteBatch batch) {
    this.batch = batch;
  }

  @Override
  public void write(String timeSeries, long timestamp, byte[] value) {
    byte[] key = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeSeries), timestamp);
    batch.put(key, value);
  }

  @Override
  public WriteBatch getData() {
    return batch;
  }
}
