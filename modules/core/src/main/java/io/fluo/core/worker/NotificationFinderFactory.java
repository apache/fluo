/*
 * Copyright 2014 Fluo authors (see AUTHORS)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.fluo.core.worker;

import io.fluo.api.config.FluoConfiguration;
import io.fluo.core.worker.finder.hash.HashNotificationFinder;
import org.apache.commons.configuration.Configuration;

public class NotificationFinderFactory {
  public static NotificationFinder newNotificationFinder(Configuration conf) {
    // this config is intentionally not public for now
    String clazz =
        conf.getString(FluoConfiguration.FLUO_PREFIX + ".worker.finder",
            HashNotificationFinder.class.getName());
    try {
      return Class.forName(clazz).asSubclass(NotificationFinder.class).newInstance();
    } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

  }
}
