package org.fluo.api.config;

import java.util.Collections;
import java.util.Map;

public class ObserverConfiguration {
  private String className;
  private Map<String,String> params = Collections.emptyMap();

  public ObserverConfiguration(String className) {
    this.className = className;
  }

  public String getClassName() {
    return className;
  }

  public ObserverConfiguration setParameters(Map<String,String> params) {
    if (params == null)
      throw new IllegalArgumentException();
    this.params = params;
    return this;
  }

  public Map<String,String> getParameters() {
    return params;
  }

}
