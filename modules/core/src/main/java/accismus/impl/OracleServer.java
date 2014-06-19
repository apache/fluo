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

import java.net.InetSocketAddress;

import accismus.impl.thrift.OracleService;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accismus.impl.thrift.OracleService;


/**
 * 
 */
public class OracleServer extends LeaderSelectorListenerAdapter implements OracleService.Iface {
  private long currentTs = 0;
  private long maxTs = 0;
  private Configuration config;
  private Thread serverThread;
  private THsHaServer server;
  private boolean started = false;

  private LeaderSelector leaderSelector;
  private CuratorFramework curatorFramework;
  
  private static Logger log = LoggerFactory.getLogger(OracleServer.class);

  public OracleServer(Configuration config) throws Exception {
    this.config = config;
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

  }
  
  @Override
  public synchronized long getTimestamps(String id, int num) throws TException {
    
    if (!started)
      throw new IllegalStateException();

    if (!id.equals(config.getAccismusInstanceID())) {
      throw new IllegalArgumentException();
    }

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
  
  private InetSocketAddress startServer() throws TTransportException {
    
    // TODO pick port and/or make configurable
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
  
  public synchronized void start() throws Exception {
    if (started)
      throw new IllegalStateException();

    InetSocketAddress addr = startServer();

    curatorFramework = CuratorFrameworkFactory.newClient(config.getConnector().getInstance().getZooKeepers(), new ExponentialBackoffRetry(1000, 10));
    curatorFramework.start();

    byte[] d = curatorFramework.getData().forPath(config.getZookeeperRoot() + Constants.Zookeeper.TIMESTAMP);
    currentTs = maxTs = Long.parseLong(new String(d));

    leaderSelector = new LeaderSelector(curatorFramework, config.getZookeeperRoot() + Constants.Zookeeper.ORACLE_SERVER, this);
    leaderSelector.setId(addr.getHostName() + ":" + addr.getPort());
    leaderSelector.start();

    log.info("Listening " + addr);

    started = true;
  }

  public void stop() throws Exception {
    if (started) {
      server.stop();
      serverThread.join();
      // TODO use zoolock or curator

      leaderSelector.close();
      curatorFramework.close();
      started = false;
    }
  }

  @Override
  public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
    while(started) Thread.sleep(50);
  }
}
