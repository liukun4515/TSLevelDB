package edu.tsinghua.k1.leveldb;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.impl.Iq80DBFactory;

/**
 * Created by liukun on 19/2/27.
 */
public class LevelTimeSeriesDBFactory implements ITimeSeriesDBFactory {

  private LevelTimeSeriesDBFactory(){

  }

  private static class Holder{
    private static LevelTimeSeriesDBFactory instance = new LevelTimeSeriesDBFactory();
  }

  public static LevelTimeSeriesDBFactory getInstance(){
    return Holder.instance;
  }
  @Override
  public ITimeSeriesDB openOrCreate(File path, Object options) throws IOException {
    DB db = Iq80DBFactory.factory.open(path, (Options) options);
    LevelTimeSeriesDB timeSeriesDB = new LevelTimeSeriesDB(path,db);
    return timeSeriesDB;
  }

  @Override
  public void destroy(File path, Object options) throws IOException {
    Iq80DBFactory.factory.destroy(path, (Options) options);
  }
}
