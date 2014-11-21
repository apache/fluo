
Fluo Stress Tests
=====================

This module contains a stress test which computes the number of unique integers 
through the process of building a bitwise trie.  New numbers are added to the trie as
leaf nodes.  Observers watch all nodes in the trie to create parents and percolate
counts up to the root nodes such that each node in the trie keeps track of the number 
of leaf nodes below it. The count at the root nodes should equal the total number of
leaf nodes.  This makes it easy to verify if the test ran correctly. The test stresses
Fluo in that multiple transactions can operate on the same data as counts are
percolated up the trie.

Concepts and definitions
------------------------

This test has the following set of configurable parameters.

 * **nodeSize** : The number of bits chopped off the end each time a number is
   percolated up.  Must choose a nodeSize such that `64 % nodeSize == 0`
 * **stopLevel** : The number of levels in the tree is a function of the
   nodeSize.  The deepest possible level is `64 / nodeSize`.  Levels are
   decremented going up the tree.  Setting the stop level determines how far up
   to percolate.  The lower the stop level, the more root nodes there are.
   Having more root nodes means less collisions, but all roots need to be
   scanned to get the count of unique numbers.  Having ~64k root nodes is a
   good choice.  
 * **max** : Random numbers are generated modulo the max. 

Setting the stop level such that you have ~64k root nodes is dependent on the
max and nodeSize.  For example assume we choose a max of 10<sup>12</sup> and a
node size of 8.  The following table shows information about each level in the
tree using this configuration.  So for a max of 10<sup>12</sup> choosing a stop
level of 5 would result in 59,604 root nodes.  With this many root nodes there
would not be many collisions and scanning 59,604 nodes to compute the unique
number of intergers is a quick operation.

|Level|Max Node             |Number of possible Nodes|
|:---:|---------------------|-----------------------:|
|  0  |`0xXXXXXXXXXXXXXXXX` |                 1      |
|  1  |`0x00XXXXXXXXXXXXXX` |                 1      |
|  2  |`0x0000XXXXXXXXXXXX` |                 1      |
|  3  |`0x000000XXXXXXXXXX` |                 1      |
|  4  |`0x000000E8XXXXXXXX` |               232      |
|  5  |`0x000000E8D4XXXXXX` |            59,604      |
|  6  |`0x000000E8D4A5XXXX` |        15,258,789      |
|  7  |`0x000000E8D4A510XX` |     3,906,250,000      |
|  8  |`0x000000E8D4A51000` | 1,000,000,000,000      |

In the table above, X indicates nibbles that are always zeroed out for every
node at that level.  You can easily view nodes at a level using a row prefix
with the fluo scan command.  For example `fluo scan -p 05` shows all nodes at
level 5.

For small scale test a max of 10<sup>9</sup> and a stop level of 6 is a good
choice. 


Run trie stress test using Mini Fluo
----------------------------------------

There are several integration tests that run the trie stress test on a MiniFluo instance.
These tests can be run using `mvn verify`.

Run trie stress test on cluster
-------------------------------

If you want to run the trie stress on the cluster, first set up HDFS, YARN, Zookeeper, 
and Accumulo. Next, initialize Fluo with following configuration:
```
io.fluo.observer.0=io.fluo.stress.trie.NodeObserver
io.fluo.app.trie.nodeSize=8
io.fluo.app.trie.stopLevel=6
```

Next, build the module:
```
mvn install
cd modules/stress
mvn package assembly:single
```

This will create two jars in target:
```
$ ls target/fluo-stress-*
target/fluo-stress-1.0.0-beta-1-SNAPSHOT.jar  
target/fluo-stress-1.0.0-beta-1-SNAPSHOT-jar-with-dependencies.jar
```

Copy fluo-stress-1.0.0-beta-1-SNAPSHOT.jar to lib/observers in your Fluo deployment:
```
cp target/fluo-stress-1.0.0-beta-1-SNAPSHOT.jar $DEPLOY/lib/observers
```

On a node where Hadoop is set up, run the following command to generate a set
of random numbers in HDFS.  This command starts a map reduce job to generate
the random integers.

```
yarn jar <jarPath> io.fluo.stress.trie.Generate <num files> <num per file> <max> <out dir>

where:

jarPath   = target/fluo-stress-1.0.0-beta-1-SNAPSHOT-jar-with-dependencies.jar
num files = Number of files to generate (and number of map task)
numPerMap = Number of random numbers to generate per file
max       = Generate random numbers between 0 and max
out dir   = Output directory
```

Before loading data, consider splitting the Accumulo table using the following
command.

```
java -cp <jarPath> io.fluo.stress.trie.Split <fluo props> <num tablets> <max>

where:

fluoProps   = Path to fluo.properties
num tablets = Num tablets to create for lowest level of tree.  Will create less tablets for higher levels based on the max.
```
After generating random numbers, load them into Fluo with one of the following
commands.  When the Fluo table is empty, you can use the command below to
initialize it using map reduce.  This simulates the case where a user has a lot
of initial data to load into Fluo.  This command should only be run when the
table is empty because it writes directly to the Fluo table w/o using
transactions.  

```
yarn jar <jarPath> io.fluo.stress.trie.Init <fluo props> <input dir> <tmp dir>

where:

input dir = A directory with file created by io.fluo.stress.trie.Generate
node size = Size of node in bits which must be a divisor of 32/64
tmp dir   = This command runs two map reduce jobs and needs an intermediate directory to store data.
```

While `Init`requires an empty Fluo table, `Load` can be run on a table with
existing data. It starts a map reduce job that executes load transactions.
Loading the same directory multiple times should not result in incorrect
counts.

```
yarn jar <jarPath> io.fluo.stress.trie.Load <fluo props> <input dir>
```

After loading data, run the following command to check the status of the
computation of the number of unique integers within Fluo.  This command will
print two numbers, the sum of the root nodes and number of root nodes.  If
there are outstanding notification to process, this count may not be accurate.


```
java -cp <jarPath> io.fluo.stress.trie.Print <fluo props>
```

In order to know how many unique numbers are expected, run the following
command.  This command runs a map reduce job that calculates the number of
unique integers.  This command can take a list of directories created by
multiple runs of `io.fluo.stress.trie.Generate`

```
yarn jar <jarPath> io.fluo.stress.trie.Unique <input dir>{ <input dir>}
```

Using these commands, one should be able to execute a test like the following.
This test scenario loads a lot of data directly into Accumulo w/o transactions
and then incrementally loads smaller amounts of data using transactions.
 
```bash
#!/bin/bash
STRESS_JAR=/changeme/fluo-stress-1.0.0-beta-1-SNAPSHOT-jar-with-dependencies.jar
FLUO_PROPS=$FLUO_HOME/conf/fluo.properties
MAX=$((10**9))
SPLITS=17
MAPS=17
REDUCES=17
GEN_INIT=$((10**6))
GEN_INCR=$((10**3))

hadoop fs -rm -r /stress/
#add splits to Fluo table
java -cp $STRESS_JAR io.fluo.stress.trie.Split $FLUO_PROPS $SPLITS $MAX

#generate and load intial data using map reduce writing directly to table
yarn jar $STRESS_JAR io.fluo.stress.trie.Generate $MAPS $((GEN_INIT / MAPS)) $MAX /stress/init
yarn jar $STRESS_JAR io.fluo.stress.trie.Init -Dmapreduce.job.reduces=$REDUCES $FLUO_PROPS /stress/init /stress/initTmp
hadoop fs -rm -r /stress/initTmp

#load data incrementally
for i in {1..10}; do
  yarn jar $STRESS_JAR io.fluo.stress.trie.Generate $MAPS $((GEN_INCR / MAPS)) $MAX /stress/$i
  yarn jar $STRESS_JAR io.fluo.stress.trie.Load $FLUO_PROPS /stress/$i
  #TODO could reload the same dataset sometimes, maybe when i%5 == 0 or something
  sleep 30
done

#print unique counts 
yarn jar $STRESS_JAR io.fluo.stress.trie.Unique -Dmapreduce.job.reduces=$REDUCES /stress/*
java -cp $STRESS_JAR io.fluo.stress.trie.Print $FLUO_PROPS 

```

