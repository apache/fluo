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

package org.apache.fluo.accumulo.util;

public class AccumuloProps {

  public static final String CLIENT_INSTANCE_NAME = "instance.name";
  public static final String CLIENT_ZOOKEEPERS = "instance.zookeepers";

  public static final String TABLE_BLOCKCACHE_ENABLED = "table.cache.block.enable";
  public static final String TABLE_CLASSPATH = "table.classpath.context";
  public static final String TABLE_DELETE_BEHAVIOR = "table.delete.behavior";
  public static final String TABLE_DELETE_BEHAVIOR_VALUE = "fail";
  public static final String TABLE_FORMATTER_CLASS = "table.formatter";
  public static final String TABLE_MAJC_RATIO = "table.compaction.major.ratio";
  public static final String VFS_CONTEXT_CLASSPATH_PROPERTY = "general.vfs.context.classpath.";
}
