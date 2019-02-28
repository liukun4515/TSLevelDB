package edu.tsinghua.k1;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBException;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;

/**
 * Created by liukun on 19/2/27.
 */
public class BaseTimeSeriesDB implements ITimeSeriesDB {

  private DB leveldb;

  public BaseTimeSeriesDB(DB db) {
    this.leveldb = db;
  }

  @Override
  public ITimeSeriesWriteBatch createBatch() {
    WriteBatch batch = leveldb.createWriteBatch();
    return new TimeSeriesBatch(batch);
  }

  @Override
  public void write(ITimeSeriesWriteBatch batch) throws TimeSeriesDBException {
    leveldb.write(batch.getData());
  }

  @Override
  public TimeSeriesDBIterator iterator(String timeSeries, long startTime, long endTime)
      throws TimeSeriesDBException {
    DBIterator dbIterator = leveldb.iterator();
    byte[] startKey = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeSeries), startTime);
    byte[] endKey = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeSeries), endTime);
    BaseTimeSeriesDBIteration timeSeriesDBIteration = new BaseTimeSeriesDBIteration(startKey,
        endKey, dbIterator);
    return timeSeriesDBIteration;
  }

  @Override
  public void close() throws IOException {
    leveldb.close();
  }
}
