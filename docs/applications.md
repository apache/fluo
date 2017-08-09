<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Fluo Applications

Once you have Fluo installed and running on your cluster, you can run Fluo applications consisting
of [clients and observers](architecture.md). This documentations will shows how to :

 * Create a Fluo client
 * Create a Fluo observer
 * Initialize a Fluo Application
 * Start and stop a Fluo application (which consists of Oracle and Worker processes)

## Fluo Maven Dependencies

For both clients and observers, you will need to include the following in your Maven pom:

```xml
<dependency>
  <groupId>org.apache.fluo</groupId>
  <artifactId>fluo-api</artifactId>
  <version>1.1.0-incubating</version>
</dependency>
<dependency>
  <groupId>org.apache.fluo</groupId>
  <artifactId>fluo-core</artifactId>
  <version>1.1.0-incubating</version>
  <scope>runtime</scope>
</dependency>
```

Fluo provides a classpath command to help users build a runtime classpath. This command along with
the `hadoop jar` command is useful when writing scripts to run Fluo client code. These commands
allow the scripts to use the versions of Hadoop, Accumulo, and Zookeeper installed on a cluster.

## Creating a Fluo client

To create a [FluoClient], you will need to provide it with a [FluoConfiguration] object that is
configured to connect to your Fluo instance.

If you have access to the [fluo-conn.properties] file that was used to configure your Fluo instance, you
can use it to build a [FluoConfiguration] object with all necessary properties:

```java
FluoConfiguration config = new FluoConfiguration(new File("fluo-conn.properties"));
config.setApplicationName("myapp");
```

You can also create an empty [FluoConfiguration] object and set properties using Java:

```java
FluoConfiguration config = new FluoConfiguration();
config.setInstanceZookeepers("localhost/fluo");
config.setApplicationName("myapp");
```

Once you have [FluoConfiguration] object, pass it to the `newClient()` method of [FluoFactory] to
create a [FluoClient]:

```java
try(FluoClient client = FluoFactory.newClient(config)){

  try (Transaction tx = client.newTransaction()) {
    // read and write some data
    tx.commit();
  }

  try (Snapshot snapshot = client.newSnapshot()) {
    //read some data
  }
}
```

It may help to reference the [API javadocs][API] while you are learning the Fluo API.

## Creating a Fluo observer

To create an observer, follow these steps:

1.  Create one or more classes that extend [Observer] like the example below. Please use [slf4j] for
    any logging in observers as [slf4j] supports multiple logging implementations. This is
    necessary as Fluo applications have a hard requirement on [logback] when running in YARN.

    ```java
    public class InvertObserver implements Observer {

      @Override
      public void process(TransactionBase tx, Bytes row, Column col) throws Exception {
        // read value
        Bytes value = tx.get(row, col);
        // invert row and value
        tx.set(value, new Column("inv", "data"), row);
      }
    }
    ```

2.  Create a class that implements [ObserverProvider] like the example below.  The purpose of this
    class is associate a set Observers with columns that trigger the observers.  The class can
    register multiple observers.

    ```java
    class AppObserverProvider implements ObserverProvider {
      @Override
      public void provide(Registry or, Context ctx) {
        //setup InvertObserver to be triggered when the column obs:data is modified
        or.forColumn(new Column("obs", "data"), NotificationType.STRONG)
          .useObserver(new InvertObserver());
        
        //Observer is a Functional interface.  So Observers can be written as lambdas.
        or.forColumn(new Column("new","data"), NotificationType.WEAK)
          .useObserver((tx,row,col) -> {
             Bytes combined = combineNewAndOld(tx,row);
             tx.set(row, new Column("current","data"), combined);
           });
      }
    }
    ```

3.  Build a jar containing these classes and include this jar in the `lib/` directory of your Fluo
    application.
4.  Configure your Fluo application to use this observer provider by modifying the Application section of
    [fluo-app.properties]. Set `fluo.observer.provider` to the observer provider class name.
5.  Initialize your Fluo application as described in the next section.  During initialization Fluo
    will obtain the observed columns from the ObserverProvider and persist the columns in Zookeeper.
    These columns persisted in Zookeeper are used by transactions to know when to trigger observers.

## Initializing a Fluo Application

Before a Fluo Application can run, it must be initiaized.  Below is an overview of what
initialization does and some of the properties that may be set for initialization.

 * **Initialize ZooKeeper** : Each application has its own area in ZooKeeper used for configuration,
   Oracle state, and worker coordination. All properties, except `fluo.connections.*`, are copied
   into ZooKeeper. For example, if `fluo.worker.num.threads=128` was set, then when a worker process
   starts it will read this from ZooKeeper.
 * **Copy Observer jars to DFS** : Fluo workers processes need the jars containing observers. These
   are provided in one of the following ways.
   * Set the property `fluo.observer.init.dir` to a local directory containing observer jars. The
     jars in this directory are copied to DFS under `<fluo.dfs.root>/<app name>`. When a worker is
     started, the jars are pulled from DFS and added to its classpath.
   * Set the property `fluo.observer.jars.url` to a directory in DFS containing observer jars.  No
     copying is done. When a worker is started, the jars are pulled from this location and added to
     its classpath.
   * Do not set any of the properties above and have the mechanism that starts the worker process
     add the needed jars to the classpath.
 * **Create Accumulo table** : Each Fluo application creates and configures an Accumulo table. The
   `fluo.accumulo.*` properties determine which Accumulo instance is used. For performance reasons,
   Fluo runs its own code in Accumulo tablet servers. Fluo attempts to copy Fluo jars into DFS and
   configure Accumulo to use them. Fluo first checks the property `fluo.accumulo.jars` and if set,
   copies the jars listed there. If that property is not set, then Fluo looks on the classpath to
   find jars. Jars are copied to a location under `<fluo.dfs.root>/<app name>`.

Below are the steps to initialize an application from the command line. It is also possible to
initialize an application using Fluo's Java API.

1. Create a copy of [fluo-app.properties] for your Fluo application. 

        cp $FLUO_HOME/conf/fluo-app.properties /path/to/myapp/fluo-app.properties

2. Edit your copy of [fluo-app.properties] and make sure to set the following:

    * Class name of your ObserverProvider
    * Paths to your Fluo observer jars
    * Accumulo configuration
    * DFS configuration

   When configuring the observer section of fluo-app.properties, you can configure your instance for the
   [phrasecount] application if you have not created your own application. See the [phrasecount]
   example for instructions. You can also choose not to configure any observers but your workers will
   be idle when started.

3. Run the command below to initialize your Fluo application. Change `myapp` to your application name:

        fluo init myapp /path/to/myapp/fluo-app.properties

   A Fluo application only needs to be initialized once. After initialization, the Fluo application
   name is used to start/stop the application and scan the Fluo table.

4. Run `fluo list` which connects to Fluo and lists applications to verify initialization.

5. Run `fluo config myapp` to see what configuration is stored in ZooKeeper.

## Starting your Fluo application

Follow the instructions below to start a Fluo application which contains an oracle and multiple workers.

1. Configure [fluo-env.sh] and [fluo-conn.properties] if you have not already.

2. Run Fluo application processes using the `fluo oracle` and `fluo worker` commands. Fluo applications
   are typically run with one oracle process and multiple worker processes. The commands below will start
   a Fluo oracle and two workers on your local machine:

        fluo oracle myapp
        fluo worker myapp
        fluo worker myapp

   The commands will retrieve your application configuration and observer jars (using your
   application name) before starting the oracle or worker process.

The oracle & worker logs can be found in the directory `logs/<applicationName>` of your Fluo installation.

If you want to distribute the processes of your Fluo application across a cluster, you will need install
Fluo on every node where you want to run a Fluo process and follow the instructions above on each node.

## Managing your Fluo application

When you have data in your Fluo application, you can view it using the command `fluo scan myapp`. 
Pipe the output to `less` using the command `fluo scan myapp | less` if you want to page through the data.

To list all Fluo applications, run `fluo list`.

To stop your Fluo application, run `jps -m | grep Fluo` to find process IDs and use `kill` to stop them.

## Running application code

The `fluo exec <app name> <class> {arguments}` provides an easy way to execute application code. It
will execute a class with a main method if a jar containing the class is included with the observer 
jars configured at initialization. When the class is run, Fluo classes and dependencies will be on 
the classpath. The `fluo exec` command can inject the applications configuration if the class is 
written in the following way. Defining the injection point is optional.

```java
import javax.inject.Inject;

public class AppCommand {

  //when run with fluo exec command, the applications configuration will be injected
  @Inject
  private static FluoConfiguration fluoConfig;

  public static void main(String[] args) throws Exception {
    try(FluoClient fluoClient = FluoFactory.newClient(fluoConfig)) {
      //do stuff with Fluo
    }
  }
}
```

## Application Configuration

For configuring observers, fluo provides a simple mechanism to set and access application specific
configuration.  See the javadoc on [FluoClient].getAppConfiguration() for more details.

## Debugging Applications

While monitoring [Fluo metrics][metrics] can detect problems (like too many transaction collisions)
in a Fluo application, [metrics][metrics] may not provide enough information to debug the root cause
of the problem. To help debug Fluo applications, low-level logging of transactions can be turned on
by setting the following loggers to TRACE:

| Logger               | Level | Information                                                                                        |
|----------------------|-------|----------------------------------------------------------------------------------------------------|
| fluo.tx            | TRACE | Provides detailed information about what transactions read and wrote                               |
| fluo.tx.summary    | TRACE | Provides a one line summary about each transaction executed                                        |
| fluo.tx.collisions | TRACE | Provides details about what data was involved When a transaction collides with another transaction |
| fluo.tx.scan       | TRACE | Provides logging for each cell read by a scan.  Scan summary logged at `fluo.tx` level.  This allows suppression of `fluo.tx.scan` while still seeing summary. |

Below is an example log after setting `fluo.tx` to TRACE. The number following `txid: ` is the
transactions start timestamp from the Oracle.

```
2015-02-11 18:24:05,341 [fluo.tx ] TRACE: txid: 3 begin() thread: 198
2015-02-11 18:24:05,343 [fluo.tx ] TRACE: txid: 3 class: com.SimpleLoader
2015-02-11 18:24:05,357 [fluo.tx ] TRACE: txid: 3 get(4333, stat count ) -> null
2015-02-11 18:24:05,357 [fluo.tx ] TRACE: txid: 3 set(4333, stat count , 1)
2015-02-11 18:24:05,441 [fluo.tx ] TRACE: txid: 3 commit() -> SUCCESSFUL commitTs: 4
2015-02-11 18:24:05,341 [fluo.tx ] TRACE: txid: 5 begin() thread: 198
2015-02-11 18:24:05,442 [fluo.tx ] TRACE: txid: 3 close()
2015-02-11 18:24:05,343 [fluo.tx ] TRACE: txid: 5 class: com.SimpleLoader
2015-02-11 18:24:05,357 [fluo.tx ] TRACE: txid: 5 get(4333, stat count ) -> 1
2015-02-11 18:24:05,357 [fluo.tx ] TRACE: txid: 5 set(4333, stat count , 2)
2015-02-11 18:24:05,441 [fluo.tx ] TRACE: txid: 5 commit() -> SUCCESSFUL commitTs: 6
2015-02-11 18:24:05,442 [fluo.tx ] TRACE: txid: 5 close()
```

The log above traces the following sequence of events.

* Transaction T1 has a start timestamp of `3`
* Thread with id `198` is executing T1, its running code from the class `com.SimpleLoader`
* T1 reads row `4333` and column `stat count` which does not exist
* T1 sets row `4333` and column `stat count` to `1`
* T1 commits successfully and its commit timestamp from the Oracle is `4`.
* Transaction T2 has a start timestamp of `5` (because its `5` > `4` it can see what T1 wrote).
* T2 reads a value of `1` for row `4333` and column `stat count`
* T2 sets row `4333` and `column `stat count` to `2`
* T2 commits successfully with a commit timestamp of `6`

Below is an example log after only setting `fluo.tx.collisions` to TRACE. This setting will only log
trace information when a collision occurs. Unlike the previous example, what the transaction read
and wrote is not logged. This shows that a transaction with a start timestamp of `106` and a class
name of `com.SimpleLoader` collided with another transaction on row `r1` and column `fam1 qual1`.

```
2015-02-11 18:17:02,639 [tx.collisions] TRACE: txid: 106 class: com.SimpleLoader
2015-02-11 18:17:02,639 [tx.collisions] TRACE: txid: 106 collisions: {r1=[fam1 qual1 ]}
```

When applications read and write arbitrary binary data, this does not log so well. In order to make
the trace logs human readable, non ASCII chars are escaped using hex. The convention used it `\xDD`
where D is a hex digit. Also the `\` character is escaped to make the output unambiguous.

[FluoFactory]: ../modules/api/src/main/java/org/apache/fluo/api/client/FluoFactory.java
[FluoClient]: ../modules/api/src/main/java/org/apache/fluo/api/client/FluoClient.java
[FluoConfiguration]: ../modules/api/src/main/java/org/apache/fluo/api/config/FluoConfiguration.java
[Observer]: ../modules/api/src/main/java/org/apache/fluo/api/observer/Observer.java
[ObserverProvider]: ../modules/api/src/main/java/org/apache/fluo/api/observer/ObserverProvider.java
[fluo-conn.properties]: ../modules/distribution/src/main/config/fluo-conn.properties
[fluo-app.properties]: ../modules/distribution/src/main/config/fluo-app.properties
[API]: https://fluo.apache.org/apidocs/
[metrics]: metrics.md
[slf4j]: http://www.slf4j.org/
[logback]: http://logback.qos.ch/
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo-env.sh]: ../modules/distribution/src/main/config/fluo-env.sh
