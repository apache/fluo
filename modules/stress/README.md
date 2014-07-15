Accismus Stress Tests
=====================

This module contains a stress test which computes the number of unique integers 
through the process of building a bitwise trie.  New numbers are added to the trie as
leaf nodes.  Observers watch all nodes in the trie to create parents and percolate
counts up to the root node such that each node in the trie keeps track of the number 
of leaf nodes below it. The count at the root node should equal the total number of
leaf nodes.  This makes it easy to verify if the test ran correctly. The test stresses
Accismus in that multiple transactions can operate on the same data as counts are
percolated up the trie.

Run trie stress test using Mini Accismus
----------------------------------------

There are several integration tests that run the trie stress test on a MiniAccismus instance.
These tests can be run using `mvn verify`.

Run trie stress test on cluster
-------------------------------

If you want to run the trie stress on the cluster, first set up HDFS, YARN, Zookeeper, 
and Accumulo. Next, initialize Accismus with following observer:
```
accismus.worker.observer.0=accismus.stress.trie.NodeObserver
```

Next, build the module:
```
cd modules/stress
mvn package assembly:single
```

This will create two jars in target:
```
$ ls target/accismus-stress-*
target/accismus-stress-1.0.0-alpha-1-SNAPSHOT.jar  
target/accismus-stress-1.0.0-alpha-1-SNAPSHOT-jar-with-dependencies.jar
```

Copy accismus-stress-1.0.0-alpha-1-SNAPSHOT.jar to lib/observers in your Accismus deployment:
```
cp target/accismus-stress-1.0.0-alpha-1-SNAPSHOT.jar $DEPLOY/lib/obervers
```

Finally, on a node where Hadoop is set up, run the following command to ingest 
data into Accismus using accismus-stress-1.0.0-alpha-1-SNAPSHOT-jar-with-dependencies.jar:

```
yarn jar <jarPath> accismus.stress.trie.NumberIngest <numMap> <numPerMap> <connPath>

where:

jarPath = target/accismus-stress-1.0.0-alpha-1-SNAPSHOT-jar-with-dependencies.jar
numMap = Number of ingest map tasks
numPerMap = Number of integers ingested per map
connPath = Path to Accismus connection.properties
```
