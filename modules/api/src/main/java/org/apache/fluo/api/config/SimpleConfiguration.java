/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.api.config;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.commons.configuration.CompositeConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.fluo.api.exceptions.FluoException;

/**
 * A simple configuration wrapper for Apache Commons configuration. The implementation supports
 * reading and writing properties style config and interpolation.
 *
 * <p>
 * This simple wrapper was created to keep 3rd party APIs out of the Fluo API.
 *
 * @since 1.0.0
 */

public class SimpleConfiguration implements Serializable {

  private static final long serialVersionUID = 1L;

  private transient Configuration internalConfig;

  private void init() {
    CompositeConfiguration compositeConfig = new CompositeConfiguration();
    compositeConfig.setThrowExceptionOnMissing(true);
    compositeConfig.setDelimiterParsingDisabled(true);
    internalConfig = compositeConfig;
  }

  public SimpleConfiguration() {
    init();
  }

  private SimpleConfiguration(Configuration subset) {
    this.internalConfig = subset;
  }

  /**
   * Read a properties style config from given file.
   */
  public SimpleConfiguration(File propertiesFile) {
    this();
    load(propertiesFile);
  }

  /**
   * Read a properties style config from given input stream.
   */
  public SimpleConfiguration(InputStream in) {
    this();
    load(in);
  }

  /**
   * Copy constructor.
   */
  public SimpleConfiguration(SimpleConfiguration other) {
    this();
    Iterator<String> iter = other.internalConfig.getKeys();
    while (iter.hasNext()) {
      String key = iter.next();
      internalConfig.setProperty(key, other.internalConfig.getProperty(key));
    }
  }

  public SimpleConfiguration(Map<String, String> map) {
    this();
    for (Entry<String, String> entry : map.entrySet()) {
      internalConfig.setProperty(entry.getKey(), entry.getValue());
    }
  }

  public void clear() {
    internalConfig.clear();
  }

  public void clearProperty(String key) {
    internalConfig.clearProperty(key);
  }

  public boolean containsKey(String key) {
    return internalConfig.containsKey(key);
  }

  public boolean getBoolean(String key) {
    return internalConfig.getBoolean(key);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    return internalConfig.getBoolean(key, defaultValue);
  }

  public int getInt(String key) {
    return internalConfig.getInt(key);
  }

  public int getInt(String key, int defaultValue) {
    return internalConfig.getInt(key, defaultValue);
  }

  public Iterator<String> getKeys() {
    return internalConfig.getKeys();
  }

  public Iterator<String> getKeys(String key) {
    return internalConfig.getKeys(key);
  }

  public long getLong(String key) {
    return internalConfig.getLong(key);
  }

  public long getLong(String key, long defaultValue) {
    return internalConfig.getLong(key, defaultValue);
  }

  /**
   * @return raw property without interpolation or null if not set.
   */
  public String getRawString(String key) {
    Object val = internalConfig.getProperty(key);
    if (val == null) {
      return null;
    }
    return val.toString();
  }

  public String getString(String key) {
    return internalConfig.getString(key);
  }

  public String getString(String key, String defaultValue) {
    return internalConfig.getString(key, defaultValue);
  }

  /**
   * Loads configuration from InputStream. Later loads have lower priority.
   * 
   * @param in InputStream to load from
   * @since 1.2.0
   */
  public void load(InputStream in) {
    try {
      PropertiesConfiguration config = new PropertiesConfiguration();
      // disabled to prevent accumulo classpath value from being shortened
      config.setDelimiterParsingDisabled(true);
      config.load(in);
      ((CompositeConfiguration) internalConfig).addConfiguration(config);
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Loads configuration from File. Later loads have lower priority.
   * 
   * @param file File to load from
   * @since 1.2.0
   */
  public void load(File file) {
    try {
      PropertiesConfiguration config = new PropertiesConfiguration();
      // disabled to prevent accumulo classpath value from being shortened
      config.setDelimiterParsingDisabled(true);
      config.load(file);
      ((CompositeConfiguration) internalConfig).addConfiguration(config);
    } catch (ConfigurationException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public void save(File file) {
    PropertiesConfiguration pconf = new PropertiesConfiguration();
    pconf.setDelimiterParsingDisabled(true);
    pconf.append(internalConfig);
    try {
      pconf.save(file);
    } catch (ConfigurationException e) {
      throw new FluoException(e);
    }
  }


  public void save(OutputStream out) {
    PropertiesConfiguration pconf = new PropertiesConfiguration();
    pconf.setDelimiterParsingDisabled(true);
    pconf.append(internalConfig);
    try {
      pconf.save(out);
    } catch (ConfigurationException e) {
      throw new FluoException(e);
    }
  }

  public void setProperty(String key, Boolean value) {
    internalConfig.setProperty(key, value);
  }

  public void setProperty(String key, Integer value) {
    internalConfig.setProperty(key, value);
  }

  public void setProperty(String key, Long value) {
    internalConfig.setProperty(key, value);
  }

  public void setProperty(String key, String value) {
    internalConfig.setProperty(key, value);
  }

  /**
   * Returns a subset of config that start with given prefix. The prefix will not be present in keys
   * of the returned config. Any changes made to the returned config will be made to this and visa
   * versa.
   */
  public SimpleConfiguration subset(String prefix) {
    return new SimpleConfiguration(internalConfig.subset(prefix));
  }

  @Override
  public String toString() {
    return ConfigurationUtils.toString(internalConfig);
  }

  /**
   * @return An immutable copy of this configurations as a map. Changes to this after toMap() is
   *         called will not be reflected in the map.
   */
  public Map<String, String> toMap() {
    Builder<String, String> builder = ImmutableMap.builder();
    Iterator<String> ki = getKeys();
    while (ki.hasNext()) {
      String k = ki.next();
      builder.put(k, getRawString(k));
    }

    return builder.build();
  }

  /*
   * These custom serialization methods were added because commons config does not support
   * serialization.
   */
  private void writeObject(ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    save(baos);

    byte[] data = baos.toByteArray();

    out.writeInt(data.length);
    out.write(data);
  }

  private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
    init();

    int len = in.readInt();
    byte[] data = new byte[len];
    in.readFully(data);

    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    load(bais);
  }
}
