[![Fluo][logo]][fluo]
---
[![Build Status][ti]][tl] [![Apache License][li]][ll] [![Maven Central][mi]][ml] [![Javadoc][ji]][jl]

**Apache Fluo lets users make incremental updates to large data sets stored in Apache Accumulo.**

[Apache Fluo][fluo] is an open source implementation of [Percolator][percolator] (which populates Google's
search index) for [Apache Accumulo][accumulo]. Fluo makes it possible to update the results of a large-scale
computation, index, or analytic as new data is discovered. Check out the Fluo [project website][fluo] for
news and general information.

### Getting Started

There are several ways to run Fluo (listed in order of increasing difficulty):

* [quickstart] - Starts a MiniFluo instance that is configured to run a word count application
* [MiniFluo] - Sets up a minimal Fluo instance that writes its data to single directory
* [fluo-dev] - Command-line tool for running Fluo and its dependencies on a single machine
* [Zetten] - Command-line tool that launches an AWS cluster and sets up Fluo/Accumulo on it
* [Production] - Sets up Fluo on a cluster where Accumulo, Hadoop & Zookeeper are running

Except for [quickstart], all above will set up a Fluo application that will be idle unless you
create client & observer code for your application. You can either [create your own
application][apps] or configure Fluo to run an example application below:

* [phrasecount] - Computes phrase counts for unique documents
* [fluo-stress] - Computes the number of unique integers by building bitwise trie
* [webindex] - Creates a web index using Common Crawl data

### Applications

Below are helpful resources for Fluo application developers:

* [Instructions][apps] for creating Fluo applications
* [Fluo API][api] javadocs
* [Fluo Recipes][recipes] is a project that provides common code for Fluo application developers implemented 
  using the Fluo API.

### Implementation

* [Architecture] - Overview of Fluo's architecture
* [Contributing] - Documentation for developers who want to contribute to Fluo
* [Metrics] - Fluo metrics are visible via JMX by default but can be configured to send to Graphite or Ganglia

[fluo]: https://fluo.apache.org/
[accumulo]: https://accumulo.apache.org
[percolator]: https://research.google.com/pubs/pub36726.html
[quickstart]: https://github.com/fluo-io/fluo-quickstart
[fluo-dev]: https://github.com/fluo-io/fluo-dev
[Zetten]: https://github.com/fluo-io/zetten
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo-stress]: https://github.com/fluo-io/fluo-stress
[webindex]: https://github.com/fluo-io/webindex
[MiniFluo]: docs/mini-fluo-setup.md
[Production]: docs/prod-fluo-setup.md
[apps]: docs/applications.md
[api]: https://fluo.apache.org/apidocs/
[recipes]: https://github.com/apache/incubator-fluo-recipes
[Metrics]: docs/metrics.md
[Contributing]: docs/contributing.md
[Architecture]: docs/architecture.md
[ti]: https://travis-ci.org/apache/incubator-fluo.svg?branch=master
[tl]: https://travis-ci.org/apache/incubator-fluo
[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://github.com/apache/incubator-fluo/blob/master/LICENSE
[mi]: https://maven-badges.herokuapp.com/maven-central/org.apache.fluo/fluo-api/badge.svg
[ml]: https://maven-badges.herokuapp.com/maven-central/org.apache.fluo/fluo-api/
[ji]: https://javadoc-emblem.rhcloud.com/doc/org.apache.fluo/fluo-api/badge.svg
[jl]: http://www.javadoc.io/doc/org.apache.fluo/fluo-api
[logo]: contrib/fluo-logo.png
