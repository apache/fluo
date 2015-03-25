<!---
Copyright 2014 Fluo authors (see AUTHORS)

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

Fluo
====

[![Build Status](https://travis-ci.org/fluo-io/fluo.svg?branch=master)](https://travis-ci.org/fluo-io/fluo)

**Fluo is transaction layer that enables incremental processsing on big data.**

Fluo is an implementation of [Percolator] built on [Accumulo] than runs in [YARN].

Fluo is not yet recommended for production use.

Check out the [Fluo project website](http://fluo.io) for news and general information.

Quickstart
----------

If you are new to Fluo, the best way to get started is to follow the [quickstart]
example which starts a local Fluo instance (called MiniFluo).  By using MiniFluo,
you can avoid configuring and running your own Fluo instance. For a more comprehensive
Fluo application, check out the [phrasecount] example.

Building Fluo
-------------

If you have [Git], [Maven], and [Java](version 7+) installed, run these commands
to build Fluo:

```
git clone https://github.com/fluo-io/fluo.git
cd fluo
mvn package
```

Running Fluo
------------

If you are new to Fluo, consider following the [quickstart] example which runs a development 
Fluo instance along with a sample Fluo application.

If you would like to install and run Fluo, follow one of the installation instructions below:

* [Test & development instructions](docs/test-dev-install.md) - sets up a MiniFluo instance that
is easy to install but should only be used for testing and development.
* [Production instructions](docs/production-install.md) - sets up a Fluo instance on local 
machine or cluster where Accumulo, Hadoop, and Zookeeper are installed and running.

Running Fluo applications
-------------------------

Once you have Fluo installed and running on your cluster, you can now run
Fluo applications. 

Fluo applications consist of clients and observers. If you are new to Fluo,
consider first building and running the [phrasecount] application on your 
Fluo instance. Otherwise, follow the [application docs](docs/applications.md)
to create your own Fluo client or observer.

Testing
-------

Fluo has a test suite that consists of the following:
* Units tests which are run by `mvn package`
* Integration tests which are run using `mvn verify`.  These tests start
a local Fluo instance (called MiniFluo) and run against it.
* A [Stress test][Stress] application designed to run on a Fluo cluster.

Metrics
-------

Fluo is instrumented using [Dropwizard metrics][Metrics].   Fluo metrics are
visible via JMX or can be configured to be sent to graphing tools like Graphite
and Ganglia.  See the [metrics documentation](docs/metrics.md) for more
information.


[Accumulo]: http://accumulo.apache.org
[Hadoop]: http://hadoop.apache.org
[Percolator]: http://research.google.com/pubs/pub36726.html
[YARN]: http://hadoop.apache.org/docs/r2.5.1/hadoop-yarn/hadoop-yarn-site/YARN.html
[Zookeeper]: http://zookeeper.apache.org/
[quickstart]: http://fluo.io/quickstart/
[phrasecount]: https://github.com/fluo-io/phrasecount
[Git]: http://git-scm.com/
[Java]: https://www.oracle.com/java/index.html
[Maven]: http://maven.apache.org/
[Metrics]: https://dropwizard.github.io/metrics/3.1.0/
[Stress]: https://github.com/fluo-io/fluo-stress
