package io.fluo.api.client;

public interface MiniFluo {

  void start();

  void stop();

  void waitForObservers();
}
