/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package accismus.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accismus.impl.support.CuratorCnxnListener;
import accismus.impl.thrift.OracleService;

import com.google.common.annotations.VisibleForTesting;

/**
 * Oracle server is the responsible for providing incrementing logical timestamps to clients. It should never
 * give the same timestamp to two clients and it should always provide an incrementing timestamp.
 * <p/>
 * If multiple oracle servers are run, they will choose a leader and clients will automatically connect
 * to that leader. If the leader goes down, the client will automatically fail over to the next leader.
 * In the case where an oracle fails over, the next oracle will begin a new block of timestamps.
 */
public class OracleServer extends LeaderSelectorListenerAdapter implements OracleService.Iface {
  private volatile long currentTs = 0;
  private volatile long maxTs = 0;
  private Configuration config;
  private Thread serverThread;
  private THsHaServer server;
  private volatile boolean started = false;

  private LeaderSelector leaderSelector;
  private CuratorFramework curatorFramework;
  private CuratorCnxnListener cnxnListener;

  private static Logger log = LoggerFactory.getLogger(OracleServer.class);

  public OracleServer(Configuration config) throws Exception {
    this.config = config;
    this.cnxnListener = new CuratorCnxnListener();
  }

  private void allocateTimestamp() throws Exception {
    Stat stat = new Stat();
    byte[] d = curatorFramework.getData()
        .storingStatIn(stat)
        .forPath(config.getZookeeperRoot() + Constants.Zookeeper.TIMESTAMP);

    // TODO check that d is expected
    // TODO check that stil server when setting
    // TODO make num allocated variable... when a server first starts allocate a small amount... the longer it runs and the busier it is, allocate bigger blocks

    long newMax = Long.parseLong(new String(d)) + 1000;

    curatorFramework.setData()
        .withVersion(stat.getVersion())
        .forPath(config.getZookeeperRoot() + Constants.Zookeeper.TIMESTAMP, (newMax + "").getBytes("UTF-8"));

    maxTs = newMax;

    if (!leaderSelector.hasLeadership())
      throw new IllegalStateException();

  }

  @Override
  public synchronized long getTimestamps(String id, int num) throws TException {

    if (!started)
      throw new IllegalStateException();

    if (!id.equals(config.getAccismusInstanceID())) {
      throw new IllegalArgumentException();
    }

    if (!leaderSelector.hasLeadership())
      throw new IllegalStateException();

    try {
      while (num + currentTs >= maxTs) {
        allocateTimestamp();
      }

      long tmp = currentTs;
      currentTs += num;

      return tmp;
    } catch (Exception e) {
      throw new TException(e);
    }
  }

  @VisibleForTesting
  public boolean isConnected() {
    return (started && cnxnListener.isConnected());
  }

  private InetSocketAddress startServer() throws TTransportException {

    InetSocketAddress addr = new InetSocketAddress(config.getOraclePort());

    TNonblockingServerSocket socket = new TNonblockingServerSocket(addr);

    THsHaServer.Args serverArgs = new THsHaServer.Args(socket);
    TProcessor processor = new OracleService.Processor<OracleService.Iface>(this);
    serverArgs.processor(processor);
    serverArgs.inputProtocolFactory(new TCompactProtocol.Factory());
    serverArgs.outputProtocolFactory(new TCompactProtocol.Factory());
    server = new THsHaServer(serverArgs);

    Runnable st = new Runnable() {

      @Override
      public void run() {
        server.serve();
      }
    };

    serverThread = new Thread(st);
    serverThread.setDaemon(true);
    serverThread.start();

    return addr;

  }
  
  private String getHostName() throws IOException {
    Process p = Runtime.getRuntime().exec("hostname");
    BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
    return reader.readLine();
  }

  public synchronized void start() throws Exception {
    if (started)
      throw new IllegalStateException();

    InetSocketAddress addr = startServer();

    curatorFramework = CuratorFrameworkFactory.newClient(config.getConnector().getInstance().getZooKeepers(), new ExponentialBackoffRetry(1000, 10));
    curatorFramework.getConnectionStateListenable().addListener(cnxnListener);
    curatorFramework.start();

    while (!cnxnListener.isConnected())
      Thread.sleep(200);

    leaderSelector = new LeaderSelector(curatorFramework, config.getZookeeperRoot() + Constants.Zookeeper.ORACLE_SERVER, this);
    String leaderId = getHostName() + ":" + addr.getPort();
    leaderSelector.setId(leaderId);
    log.info("Leader ID = " + leaderId);
    leaderSelector.autoRequeue();
    leaderSelector.start();

    log.info("Listening " + addr);

    started = true;
  }

  public void stop() throws Exception {
    if (started) {
      server.stop();
      serverThread.join();

      if (curatorFramework.getState().equals(CuratorFrameworkState.STARTED)) {

        leaderSelector.close();
        curatorFramework.close();
      }
      started = false;
      log.debug("Oracle server is stopped.");
    }
  }

  /**
   * Upon an oracle being elected the leader, it will need to adjust its starting timestamp to the last timestamp
   * set in zookeeper.
   *
   * @param curatorFramework
   * @throws Exception
   */
  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {

    // TODO when we first get leadership should we delay processing request for a bit? its possible the old oracle process is still out there
    // and could be processing request for a short period..

    synchronized (this) {
      byte[] d = curatorFramework.getData().forPath(config.getZookeeperRoot() + Constants.Zookeeper.TIMESTAMP);
      currentTs = maxTs = Long.parseLong(new String(d));
    }

    while (started)
      Thread.sleep(100); // if leadership is lost, then curator will interrupt the thread that called this method
  }

  @Override public void stateChanged(CuratorFramework client, ConnectionState newState) {
    super.stateChanged(client, newState);

    if(newState.equals(ConnectionState.RECONNECTED))
      leaderSelector.requeue();
  }
}
