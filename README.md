Accismus
========

A [Percolator][2] prototype  for [Accumulo][1].  This prototype relies on the
[ACCUMULO-1000][3] [branch][4].

Building Accumulo 1.6.0 with CAS
--------------------------------

Before you can build Accissmus, you will need to build Accumulo with CAS.  This
can be accomplished with the following steps.

```
svn checkout  https://svn.apache.org/repos/asf/accumulo/branches/ACCUMULO-1000
cd ACCUMULO-1000/
mvn clean compile install -DskipTests
```

Running these commands will leave 1.6.0-SNAPSHOT jars in your local maven repo.
You may want to delete these later.


Building Accismus
-----------------

After building Accumulo with CAS, you can build Accismus with the following steps.

```
git clone https://github.com/keith-turner/Accismus.git
cd Accismus
mvn package
```

Experimenting with Accismus
---------------------------

An easy way to experiment with Accismus is to edit a unit test and run it.  The
WorkerTest is probably a good place to start because it uses an Observer to
build a secondary index.  This can be done using the following commands.  

```
vim src/test/java/org/apache/accumulo/accismus/WorkerTest.java
mvn test -Dtest=org.apache.accumulo.accismus.WorkerTest
```

[1]: http://accumulo.apache.org
[2]: http://research.google.com/pubs/pub36726.html
[3]: https://issues.apache.org/jira/browse/ACCUMULO-1000
[4]: https://svn.apache.org/repos/asf/accumulo/branches/ACCUMULO-1000


