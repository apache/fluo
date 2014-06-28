package io.fluo.api.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class OracleProperties extends ConnectionProperties {
 
  private static final long serialVersionUID = 1L;
 
  public static final String ORACLE_PORT_PROP = "io.fluo.oracle.port";
  public static final String ORACLE_MAX_MEMORY_PROP = "io.fluo.oracle.max.memory.mb";

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
    setProperty(OracleProperties.ORACLE_PORT_PROP, Integer.toString(oraclePort));
    return this;
  }
  
  private void setDefaults() {
    setDefault(ORACLE_PORT_PROP, ORACLE_DEFAULT_PORT+"");
  }
}
