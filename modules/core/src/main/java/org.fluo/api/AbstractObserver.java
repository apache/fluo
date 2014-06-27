package org.fluo.api;

import java.util.Map;

public abstract class AbstractObserver implements Observer {

  @Override
  public void init(Map<String,String> config) throws Exception {}

  @Override
  public void close() {}

}
