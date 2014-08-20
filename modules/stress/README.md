Fluo Stress Tests
=====================

This module contains a stress test which computes the number of unique integers 
through the process of building a bitwise trie.  New numbers are added to the trie as
leaf nodes.  Observers watch all nodes in the trie to create parents and percolate
counts up to the root node such that each node in the trie keeps track of the number 
of leaf nodes below it. The count at the root node should equal the total number of
leaf nodes.  This makes it easy to verify if the test ran correctly. The test stresses
Fluo in that multiple transactions can operate on the same data as counts are
percolated up the trie.

Run trie stress test using Mini Fluo
----------------------------------------

There are several integration tests that run the trie stress test on a MiniFluo instance.
These tests can be run using `mvn verify`.

Run trie stress test on cluster
-------------------------------

If you want to run the trie stress on the cluster, first set up HDFS, YARN, Zookeeper, 
and Accumulo. Next, initialize Fluo with following observer:
```
io.fluo.observer.0=io.fluo.stress.trie.NodeObserver
```

Next, build the module:
```
cd modules/stress
mvn package assembly:single
```

This will create two jars in target:
```
$ ls target/fluo-stress-*
target/fluo-stress-1.0.0-alpha-1-SNAPSHOT.jar  
target/fluo-stress-1.0.0-alpha-1-SNAPSHOT-jar-with-dependencies.jar
```

Copy fluo-stress-1.0.0-alpha-1-SNAPSHOT.jar to lib/observers in your Fluo deployment:
```
cp target/fluo-stress-1.0.0-alpha-1-SNAPSHOT.jar $DEPLOY/lib/observers
```

Finally, on a node where Hadoop is set up, run the following command to ingest 
data into Fluo using fluo-stress-1.0.0-alpha-1-SNAPSHOT-jar-with-dependencies.jar:

```
yarn jar <jarPath> io.fluo.stress.trie.NumberIngest <numMap> <numPerMap> <nodeSize> <fluoProps>

where:

jarPath = target/fluo-stress-1.0.0-alpha-1-SNAPSHOT-jar-with-dependencies.jar
numMap = Number of ingest map tasks
numPerMap = Number of integers ingested per map
nodeSize = Size of node in bits which must be a divisor of 32/64
fluoProps = Path to fluo.properties
```
