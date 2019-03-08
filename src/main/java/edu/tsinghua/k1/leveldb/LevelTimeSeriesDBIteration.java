package edu.tsinghua.k1.leveldb;

import edu.tsinghua.k1.api.TimeSeriesDBException;
import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.util.Comparator;
import java.util.Map.Entry;
import org.iq80.leveldb.DBIterator;

/**
 * Created by liukun on 19/2/27.
 */
public class LevelTimeSeriesDBIteration implements TimeSeriesDBIterator {

  // key比较器
  // key：timeseries identifier `+` timestamp
  // timeseries identifier的高字节存储在byte array的低字节中，timestamp同理
  // negative value:  o1 < o2
  // zero value: o1=o2
  // positive value: o1>o2
  private static Comparator<byte[]> keyComparator = new Comparator<byte[]>() {
    @Override
    public int compare(byte[] o1, byte[] o2) {
      for (int i = 0; i < o1.length; i++) {
        if (o1[i] != o2[i]) {
          return o1[i] - o2[i];
        }
      }
      return 0;
    }
  };

  private byte[] startKey;
  private byte[] endKey;
  private DBIterator iterator;
  private Entry<byte[], byte[]> value;

  public LevelTimeSeriesDBIteration(byte[] startKey, byte[] endKey, DBIterator iterator) {
    this.startKey = startKey;
    this.endKey = endKey;
    this.iterator = iterator;
    // seek to the start key
    this.iterator.seek(startKey);
    // Get the first value
    getNext();
  }

  private void getNext() {
    // reset the value
    value = null;
    if (iterator.hasNext()) {
      // get new value
      value = iterator.next();
      // next key > end key
      if (keyComparator.compare(value.getKey(), endKey) > 0) {
        value = null;
      }
    }
  }

  @Override
  public boolean hasNext() {
    return value != null;
  }

  @Override
  public Entry<byte[], byte[]> next() {
    if (value == null) {
      throw new TimeSeriesDBException("TimeSeriesDBIterator has no more data");
    }
    Entry<byte[], byte[]> result = value;
    getNext();
    return result;
  }
}
