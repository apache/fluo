package io.fluo.api.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class OracleProperties extends ConnectionProperties {
 
  private static final long serialVersionUID = 1L;
 
  public static final String ORACLE_PORT_PROP = FLUO_PREFIX + ".oracle.port";
  public static final String ORACLE_MAX_MEMORY_PROP = FLUO_PREFIX + ".oracle.max.memory.mb";

  public static final int ORACLE_DEFAULT_PORT = 9913;
  
  public OracleProperties() {
    super();
    setDefaults();
  }

  public OracleProperties(File file) throws FileNotFoundException, IOException {
    this();
    load(new FileInputStream(file));
    setDefaults();
  }

  public OracleProperties(Properties props) {
    super(props);
    setDefaults();
  }

  public OracleProperties setOracleMaxMemory(String oracleMaxMemory) {
    setProperty(OracleProperties.ORACLE_MAX_MEMORY_PROP, oracleMaxMemory);
    return this;
  }

  public OracleProperties setOraclePort(int oraclePort) {
    setOraclePort(this, oraclePort);
    return this;
  }

  public static void setOraclePort(Properties props, int oraclePort) {
    props.setProperty(OracleProperties.ORACLE_PORT_PROP, Integer.toString(oraclePort));
  }
  
  private void setDefaults() {
    setDefaultOracleProp(this);
  }

  public static void setDefaultOracleProp(Properties props) {
    props.setProperty(ORACLE_PORT_PROP, ORACLE_DEFAULT_PORT+"");
  }
}
