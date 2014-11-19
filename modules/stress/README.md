
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
java -cp <jarPath> io.fluo.stress.trie.Split <fluo props> <num tablets>

where:

fluoProps   = Path to fluo.properties
num tablets = Num tablets to create for lowest level of tree.  May create less tablets for higher levels.
```
After generating random numbers, load them into Fluo with one of the following
commands.  When the Fluo table is empty, you can use the command below to
initialize it using map reduce.  This simulates the case where a user has a lot
of initial data to load into Fluo.  This command should only be run when the
table is empty because it writes directly to the Fluo table w/o using
transactions.  

```
yarn jar <jarPath> io.fluo.stress.trie.Init <nodeSize> <fluo props> <input dir> <tmp dir>

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
yarn jar <jarPath> io.fluo.stress.trie.Load <node size> <fluo props> <input dir>
```

After loading data, run the following command to check the status of the
computation of the number of unique integers within Fluo.  This command will
print two numbers, the count at the root and the sum of all of the numbers
working their way to the root.  The sum of these two numbers should always
equal the number of unique integers loaded into fluo.  This command is not a
map reduce job, so it could take a awhile on larger data sets.

```
java -cp <jarPath> io.fluo.stress.trie.Print <fluo props> <node size>
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
NODE_SIZE=8
MAX=$((10**9))
SPLITS=68
MAPS=17
REDUCES=17
GEN_INIT=$((10**6))
GEN_INCR=$((10**3))

hadoop fs -rm -r /stress/
#add splits to Fluo table
java -cp $STRESS_JAR io.fluo.stress.trie.Split $FLUO_PROPS $SPLITS

#generate and load intial data using map reduce writing directly to table
yarn jar $STRESS_JAR io.fluo.stress.trie.Generate $MAPS $((GEN_INIT / MAPS)) $MAX /stress/init
yarn jar $STRESS_JAR io.fluo.stress.trie.Init -Dmapreduce.job.reduces=$REDUCES $NODE_SIZE $FLUO_PROPS\ 
         /stress/init /stress/initTmp
hadoop fs -rm -r /stress/initTmp

#load data incrementally
for i in {1..10}; do
  yarn jar $STRESS_JAR io.fluo.stress.trie.Generate $MAPS $((GEN_INCR / MAPS)) $MAX /stress/$i
  yarn jar $STRESS_JAR io.fluo.stress.trie.Load $NODE_SIZE $FLUO_PROPS /stress/$i
  #TODO could reload the same dataset sometimes, maybe when i%5 == 0 or something
  sleep 30
done

#print unique counts 
yarn jar $STRESS_JAR io.fluo.stress.trie.Unique -Dmapreduce.job.reduces=$REDUCES /stress/*
java -cp $STRESS_JAR io.fluo.stress.trie.Print $FLUO_PROPS $NODE_SIZE

```

