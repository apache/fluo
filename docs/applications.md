Creating Fluo applications
==========================

Once you have Fluo installed and running on your cluster, you can now run Fluo applications which consist of 
clients and observers.

If you are new to Fluo, consider first running the [phrasecount] application on your Fluo instance.  Otherwise,
you can create your own Fluo client or observer by the following the steps below.
 
For both clients and observers, you will need to include the following in your Maven pom:

```xml
<dependency>
  <groupId>io.fluo</groupId>
  <artifactId>fluo-api</artifactId>
  <version>1.0.0-beta-1</version>
</dependency>
<dependency>
  <groupId>io.fluo</groupId>
  <artifactId>fluo-core</artifactId>
  <version>1.0.0-beta-1</version>
  <scope>runtime</scope>
</dependency>
```

Fluo provides a classpath command to help users build a runtime classpath.
This command along with the `hadoop jar` command is useful when writing
scripts to run Fluo client code.  These command allow the scripts to use the
versions of Hadoop, Accumulo, and Zookeeper installed on a cluster.
 
Creating a Fluo client
----------------------

To create a [FluoClient], you will need to provide it with a [FluoConfiguration] object that is configured
to connect to your Fluo instance.

If you have access to the [fluo.properties] file that was used to configure your Fluo instance, you can use
it to build a [FluoConfiguration] object with all necessary properties which are all properties with the 
`io.fluo.client.*` prefix in [fluo.properties]:

```java
FluoConfiguration config = new FluoConfiguration(new File("fluo.properties"));
```

You can also create an empty [FluoConfiguration] object and set properties using Java:

```java
FluoConfiguration config = new FluoConfiguration();
config.setAccumuloUser("user");
config.setAccumuloPassword("pass");
config.setAccumuloInstance("instance");
```

Once you have [FluoConfiguration] object, pass it to the `newClient()` method of [FluoFactory] to create a [FluoClient]:

```java
FluoClient client = FluoFactory.newClient(config)
```

It may help to reference the [API javadocs][API] while you are learning the Fluo API.

Creating a Fluo observer
------------------------

To create an observer, follow these steps:

1. Create a class that extends [AbstractObserver].
2. Build a jar containing this class and include this jar in the `lib/` directory of your Fluo application.
3. Configure your Fluo instance to use this observer by modifying the Observer section of [fluo.properties].  
4. Restart your Fluo instance so that your Fluo workers load the new observer.

Application Configuration
-------------------------

Each observer can have its own configuration.  This is useful for the case of
using the same observer code w/ different parameters.  However for the case of
sharing the same configuration across observers, fluo provides a simple
mechanism to set and access application specific configuration.  See the
javadoc on [FluoClient].getAppConfiguration() for more details.

Debugging Applications
======================

While monitoring [Fluo metrics][metrics] can detect problems (like too many
transaction collisions) in a Fluo application, [metrics][metrics] may not
provide enough information to debug the root cause of the problem.  To help
debug Fluo applications, low-level logging of transactions can be turned on by
setting the following loggers to TRACE:


| Logger                  | Level | Information                                                                                        |
|-------------------------|-------|----------------------------------------------------------------------------------------------------|
| `io.fluo.tx`            | TRACE | Provides detailed information about what transactions read and wrote                               |
| `io.fluo.tx.summary`    | TRACE | Provides a one line summary about each transaction executed                                        |
| `io.fluo.tx.collisions` | TRACE | Provides details about what data was involved When a transaction collides with another transaction |

Below is an example log after setting `io.fluo.tx` to TRACE.   The number
following `txid: ` is the transactions start timestamp from the Oracle.  

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

Below is an example log after only setting `io.fluo.tx.collisions` to TRACE.
This setting will only log trace information when a collision occurs.  Unlike
the previous example, what the transaction read and wrote is not logged.  This
shows that a transaction with a start timestamp of `106` and a class name of
`com.SimpleLoader` collided with another transaction on row `r1` and column
`fam1 qual1`.

```
2015-02-11 18:17:02,639 [tx.collisions] TRACE: txid: 106 class: com.SimpleLoader
2015-02-11 18:17:02,639 [tx.collisions] TRACE: txid: 106 collisions: {r1=[fam1 qual1 ]}
```

[phrasecount]: https://github.com/fluo-io/phrasecount
[FluoFactory]: ../modules/api/src/main/java/io/fluo/api/client/FluoFactory.java
[FluoClient]: ../modules/api/src/main/java/io/fluo/api/client/FluoClient.java
[FluoConfiguration]: ../modules/api/src/main/java/io/fluo/api/config/FluoConfiguration.java
[AbstractObserver]: ../modules/api/src/main/java/io/fluo/api/observer/AbstractObserver.java
[fluo.properties]: ../modules/distribution/src/main/config/fluo.properties
[API]: http://fluo.io/apidocs/
[metrics]: metrics.md
