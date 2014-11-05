/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fluo.core.metrics;

import io.fluo.api.config.FluoConfiguration;

public class MetricNames {

  public static final String ORCALE_CLIENT_STAMPS = FluoConfiguration.FLUO_PREFIX + ".oracle.client.stamps";
  public static final String ORACLE_CLIENT_GET_STAMPS = FluoConfiguration.FLUO_PREFIX + ".oracle.client.rpc.getStamps.time";
  public static final String ORACLE_SERVER_STAMPS = FluoConfiguration.FLUO_PREFIX + ".oracle.server.stamps";

  public static final String TX_PREFIX = FluoConfiguration.FLUO_PREFIX + ".tx.";
  public static final String TX_LOCKWAIT = MetricNames.TX_PREFIX + "lockWait.";
  public static final String TX_TIME = MetricNames.TX_PREFIX + "time.";
  public static final String TX_COLLISIONS = MetricNames.TX_PREFIX + "collisions.";
  public static final String TX_SET = MetricNames.TX_PREFIX + "set.";
  public static final String TX_READ = MetricNames.TX_PREFIX + "read.";
  public static final String TX_LOCKS_TIMEDOUT = MetricNames.TX_PREFIX + "locks.timedout.";
  public static final String TX_LOCKS_DEAD = MetricNames.TX_PREFIX + "locks.dead.";
  public static final String TX_STATUS = MetricNames.TX_PREFIX + "status.";
  
  
}
