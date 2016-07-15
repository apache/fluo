Contributing to Fluo
====================

Building Fluo
-------------

If you have [Git], [Maven], and [Java][java] (version 8+) installed, run these commands
to build Fluo:

    git clone https://github.com/apache/incubator-fluo.git
    cd fluo
    mvn package

Testing Fluo
------------

Fluo has a test suite that consists of the following:

* Units tests which are run by `mvn package`
* Integration tests which are run using `mvn verify`.  These tests start
a local Fluo instance (called MiniFluo) and run against it.
* A [Stress test][Stress] application designed to run on a cluster.

[Git]: http://git-scm.com/
[java]: http://openjdk.java.net/
[Maven]: http://maven.apache.org/
[Stress]: https://github.com/fluo-io/fluo-stress
