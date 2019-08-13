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
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import org.apache.commons.configuration2.CompositeConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.fluo.api.exceptions.FluoException;

import static java.nio.charset.StandardCharsets.UTF_8;

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
      config.getLayout().load(config, checkProps(in));
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
    try (InputStream in = Files.newInputStream(file.toPath())) {
      PropertiesConfiguration config = new PropertiesConfiguration();
      config.getLayout().load(config, checkProps(in));
      ((CompositeConfiguration) internalConfig).addConfiguration(config);
    } catch (ConfigurationException | IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public void save(File file) {
    try (Writer writer = Files.newBufferedWriter(file.toPath())) {
      PropertiesConfiguration pconf = new PropertiesConfiguration();
      pconf.append(internalConfig);
      pconf.getLayout().save(pconf, writer);
    } catch (ConfigurationException | IOException e) {
      throw new FluoException(e);
    }
  }

  public void save(OutputStream out) {
    try {
      PropertiesConfiguration pconf = new PropertiesConfiguration();
      pconf.append(internalConfig);
      pconf.getLayout().save(pconf, new OutputStreamWriter(out, UTF_8));
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

  /**
   * @param fallback SimpleConfiguration to join together
   * @return a new simple configuration that contains all of the current properties from this plus
   *         the properties from fallback that are not present in this.
   * 
   * @since 1.2.0
   */
  public SimpleConfiguration orElse(SimpleConfiguration fallback) {
    SimpleConfiguration copy = new SimpleConfiguration(this);
    for (Map.Entry<String, String> entry : fallback.toMap().entrySet()) {
      if (!copy.containsKey(entry.getKey())) {
        copy.setProperty(entry.getKey(), entry.getValue());
      }
    }
    return copy;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.toMap().entrySet());
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) {
      return true;
    }

    if (o instanceof SimpleConfiguration) {
      Map<String, String> th = this.toMap();
      Map<String, String> sc = ((SimpleConfiguration) o).toMap();
      if (th.size() == sc.size()) {
        return th.entrySet().equals(sc.entrySet());
      }
    }
    return false;
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

  private String stream2String(InputStream in) {
    try {
      ByteArrayOutputStream result = new ByteArrayOutputStream();
      byte[] buffer = new byte[4096];
      int length;
      while ((length = in.read(buffer)) != -1) {
        result.write(buffer, 0, length);
      }

      return result.toString(UTF_8.name());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /*
   * Commons config 1 was used previously to implement this class. Commons config 1 required
   * escaping interpolation. This escaping is no longer required with commmons config 2. If
   * interpolation is escaped, then this API behaves differently. This function suppresses escaped
   * interpolation in order to maintain behavior for reading.
   */
  private Reader checkProps(InputStream in) {
    String propsData = stream2String(in);
    if (propsData.contains("\\${")) {
      throw new IllegalArgumentException(
          "A Fluo properties value contains \\${.  In the past Fluo used Apache Commons Config 1 and this was required for "
              + "interpolation.  Fluo now uses Commons Config 2 and this is no longer required.  Please remove the slash "
              + "preceding the interpolation.");
    }

    return new StringReader(propsData);
  }
}
