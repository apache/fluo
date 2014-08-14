package io.fluo.api.config;

public class MiniFluoProperties extends InitializationProperties {

  public static final String MINI_CLASS_PROP = FLUO_PREFIX + ".mini.class";
  public static final String DEFAULT_MINI_CLASS = FLUO_PREFIX + ".core.client.MiniFluoImpl";

  public MiniFluoProperties() {
    super();
    setDefaults();
  }

  public MiniFluoProperties setOraclePort(int port) {
    OracleProperties.setOraclePort(this, port);
    return this;
  }

  private void setDefaults() {
    setProperty(MINI_CLASS_PROP, DEFAULT_MINI_CLASS);
    OracleProperties.setDefaultOracleProp(this);
  }
}
