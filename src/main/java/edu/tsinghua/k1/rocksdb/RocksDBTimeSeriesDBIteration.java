package edu.tsinghua.k1.rocksdb;

import edu.tsinghua.k1.api.TimeSeriesDBIterator;
import java.util.Comparator;
import java.util.Map.Entry;
import org.rocksdb.RocksIterator;

/**
 * Created by liukun on 19/3/12.
 */
public class RocksDBTimeSeriesDBIteration implements TimeSeriesDBIterator {

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
  private RocksIterator iterator;
  private Entry<byte[], byte[]> value;

  public RocksDBTimeSeriesDBIteration(byte[] startKey, byte[] endKey, RocksIterator iterator) {
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
    if (iterator.isValid()) {
      value = new Entry<byte[], byte[]>() {
        private byte[] key;
        private byte[] value;

        @Override
        public byte[] getKey() {
          this.key = iterator.key();
          return key;
        }

        @Override
        public byte[] getValue() {
          this.value = iterator.value();
          return value;
        }

        @Override
        public byte[] setValue(byte[] value) {
          this.value = value;
          return value;
        }
      };
      // next key > end key
      if (keyComparator.compare(value.getKey(), endKey) > 0) {
        value = null;
      }
      // get new value
      iterator.next();
    }
  }


  @Override
  public boolean hasNext() {
    return value != null;
  }

  @Override
  public Entry<byte[], byte[]> next() {
    return value;
  }
}
