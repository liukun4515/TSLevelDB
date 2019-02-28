package edu.tsinghua.k1;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by liukun on 19/2/27.
 */
public class UIDAllocator {

  private AtomicInteger idGenerator;

  private UIDAllocator(){
    idGenerator = new AtomicInteger(1);
  }

  public int getId(){
    return idGenerator.getAndIncrement();
  }

  private static class InstanceHolder{
    private static UIDAllocator instance = new UIDAllocator();
  }
  public static UIDAllocator getInstance(){
    return InstanceHolder.instance;
  }
}
