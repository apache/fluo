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

package org.apache.fluo.core.metrics;

import java.util.concurrent.TimeUnit;

import org.apache.fluo.api.metrics.Counter;
import org.apache.fluo.api.metrics.Histogram;
import org.apache.fluo.api.metrics.Meter;
import org.apache.fluo.api.metrics.MetricsReporter;
import org.apache.fluo.api.metrics.Timer;

public class DummyMetricsReporter implements MetricsReporter {

  @Override
  public Counter counter(String name) {
    return new DummyCounter();
  }

  @Override
  public Histogram histogram(String name) {
    return new DummyHistogram();
  }

  @Override
  public Meter meter(String name) {
    return new DummyMeter();
  }

  @Override
  public Timer timer(String name) {
    return new DummyTimer();
  }

  class DummyCounter implements Counter {

    @Override
    public void inc() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void inc(long value) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dec() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void dec(long value) {
      throw new UnsupportedOperationException();
    }
  }

  class DummyHistogram implements Histogram {

    @Override
    public void update(long value) {
      throw new UnsupportedOperationException();
    }
  }

  class DummyMeter implements Meter {

    @Override
    public void mark() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void mark(long numEvents) {
      throw new UnsupportedOperationException();
    }
  }

  class DummyTimer implements Timer {

    @Override
    public void update(long duration, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }
  }
}
