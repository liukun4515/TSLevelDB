package edu.tsinghua.k1.leveldb;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.TimeSeriesMap;
import edu.tsinghua.k1.UIDAllocator;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBException;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteBatch;

/**
 * Created by liukun on 19/2/27.
 */
public class LevelTimeSeriesDB implements ITimeSeriesDB {

  private DB leveldb;
  private File indexFile;

  public LevelTimeSeriesDB(File path, DB db) {
    this.leveldb = db;
    // deserialize index data
    indexFile = new File(path, "ts_id.index");
    UIDAllocator.getInstance().deserialize(indexFile);
    TimeSeriesMap.getInstance().deserialize(indexFile);
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
    // serialize index data
    indexFile.delete();
    UIDAllocator.getInstance().serialize(indexFile);
    TimeSeriesMap.getInstance().serialize(indexFile);
    leveldb.close();
  }
}
