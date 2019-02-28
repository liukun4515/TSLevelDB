# TSLevelDB

> 时间序列数据库使用leveldb作为存储引擎

## 设计实现

leveldb是一个基于key-value的存储引擎，为了使用leveldb来存储时间序列数据，我们需要将时间序列的存储模型映射到key-value当中。

### 映射关系

由于leveldb是一个key-value数据存储引擎，并且key与value都是直接使用字节数组进行表示，因此需要将数据转化为字节数据的方式存储。

key的表示：timeseries identifier `+` timestamp

 - timeseries identifier是一个unsigned int32转化后的byte array（每一条timeseries都对应一个独一无二的unsigned int32）

 - timestamp是一个unsigned int64转化后的byte array

value的表示：value

 - value是一个字符串对应的byte array（UTF-8字符集，big-endien)

## 提供接口

#### 创建和销毁数据库

- 创建数据库：

```
// 创建数据库获得数据库实例
File file = new File("timeseries-leveldb-example");
    Options options = new Options();
    options.createIfMissing(true);
    // 根据需求配置options

    // 创建time series db
    ITimeSeriesDB timeSeriesDB = null;
    try {
      timeSeriesDB = BaseTimeSeriesDBFactory.getInstance().openOrCreate(file, options);
    }finally{
      // if the is not null, close it
    }
```

```
// 销毁数据库实例
// destroy
    BaseTimeSeriesDBFactory.getInstance().destroy(file,options);
```

#### 写入数据

```
      ITimeSeriesWriteBatch batch = timeSeriesDB.createBatch();
      // write data to batch
      String timeseries = "root.g1.s1";
      long timestamp = 1111111;
      byte[] bytes = new byte[10];
      batch.write(timeseries,timestamp,bytes);
      // write batch to DB
      timeSeriesDB.write(batch);
```

#### 查询数据

```
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
```
