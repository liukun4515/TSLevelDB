package edu.tsinghua.k1.rocksdb;

import edu.tsinghua.k1.ByteUtils;
import edu.tsinghua.k1.TimeSeriesMap;
import edu.tsinghua.k1.UIDAllocator;
import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesWriteBatch;
import edu.tsinghua.k1.api.TimeSeriesDBException;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.io.File;
import java.io.IOException;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Created by liukun on 19/3/12.
 */
public class RocksDBTimeSeriesDB implements ITimeSeriesDB {

  private RocksDB db;
  private File indexFile;

  public RocksDBTimeSeriesDB(File path, RocksDB db) {
    this.db = db;
    this.indexFile = new File(path, "ts_id.index");
    UIDAllocator.getInstance().deserialize(indexFile);
    TimeSeriesMap.getInstance().deserialize(indexFile);
  }

  @Override
  public ITimeSeriesWriteBatch createBatch() {
    ITimeSeriesWriteBatch batch = new RocksTimeSeriesBatch(new WriteBatch());
    return batch;
  }

  @Override
  public void write(ITimeSeriesWriteBatch batch) throws TimeSeriesDBException {
    WriteOptions writeOptions = new WriteOptions();
    try {
      this.db.write(writeOptions, (WriteBatch) batch.getData());
    } catch (RocksDBException e) {
      e.printStackTrace();
      throw new TimeSeriesDBException(e);
    }finally {
      // clear for c++ object
      writeOptions.close();
    }
  }

  @Override
  public TimeSeriesDBIterator iterator(String timeSeries, long startTime, long endTime)
      throws TimeSeriesDBException {
    byte[] startKey = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeSeries), startTime);
    byte[] endKey = ByteUtils.getKey(TimeSeriesMap.getInstance().getUid(timeSeries), endTime);
    TimeSeriesDBIterator iterator = new RocksDBTimeSeriesDBIteration(startKey, endKey,
        this.db.newIterator());
    return iterator;
  }

  @Override
  public void close() throws IOException {
    indexFile.delete();
    UIDAllocator.getInstance().serialize(indexFile);
    TimeSeriesMap.getInstance().serialize(indexFile);
    db.close();
  }
}
