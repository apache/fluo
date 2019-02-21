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

package org.apache.fluo.accumulo.summarizer;

import java.util.Map;

import com.google.common.base.Preconditions;
import org.apache.accumulo.core.client.summary.Summarizer;
import org.apache.accumulo.core.client.summary.SummarizerConfiguration;
import org.apache.accumulo.core.client.summary.Summary;

public class FluoSummarizer implements Summarizer {

  public static final SummarizerConfiguration CONFIG =
      SummarizerConfiguration.builder(FluoSummarizer.class).setPropertyId("fluo").build();

  @Override
  public Collector collector(SummarizerConfiguration sc) {
    return new FluoCollector();
  }

  @Override
  public Combiner combiner(SummarizerConfiguration sc) {
    return (m1, m2) -> m2.forEach((k, v) -> m1.merge(k, v, Long::sum));
  }

  public static class Counts {

    public final long ntfy;
    public final long ntfyDel;
    public final long txDone;
    public final long delLock;
    public final long lock;
    public final long data;
    public final long write;
    public final long ack;
    public final long delrlock;
    public final long rlock;

    public Counts(long ntfy, long ntfyDel, long txDone, long delLock, long lock, long data,
        long write, long ack, long delrlock, long rlock) {
      this.ntfy = ntfy;
      this.ntfyDel = ntfyDel;
      this.txDone = txDone;
      this.delLock = delLock;
      this.lock = lock;
      this.data = data;
      this.write = write;
      this.ack = ack;
      this.delrlock = delrlock;
      this.rlock = rlock;
    }
  }

  public static Counts getCounts(Summary summary) {
    Preconditions.checkArgument(
        summary.getSummarizerConfiguration().getClassName().equals(FluoSummarizer.class.getName()));
    Map<String, Long> m = summary.getStatistics();
    return new Counts(m.get("ntfy"), m.get("ntfyDel"), m.get("txDone"), m.get("delLock"),
        m.get("lock"), m.get("data"), m.get("write"), m.get("ack"), m.get("delrlock"),
        m.get("rlock"));
  }
}
