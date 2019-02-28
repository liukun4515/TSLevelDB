package edu.tsinghua.k1;

import edu.tsinghua.k1.api.ITimeSeriesDB;
import edu.tsinghua.k1.api.ITimeSeriesDBFactory;
import java.io.File;
import java.io.IOException;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

/**
 * Created by liukun on 19/2/27.
 */
public class BaseTimeSeriesDBFactory implements ITimeSeriesDBFactory {

  private BaseTimeSeriesDBFactory(){

  }

  private static class Holder{
    private static BaseTimeSeriesDBFactory instance = new BaseTimeSeriesDBFactory();
  }

  public static BaseTimeSeriesDBFactory getInstance(){
    return Holder.instance;
  }
  @Override
  public ITimeSeriesDB openOrCreate(File path, Options options) throws IOException {
    DB db = JniDBFactory.factory.open(path,options);
    BaseTimeSeriesDB timeSeriesDB = new BaseTimeSeriesDB(db);
    return timeSeriesDB;
  }

  @Override
  public void destroy(File path, Options options) throws IOException {
    JniDBFactory.factory.destroy(path,options);
  }
}
