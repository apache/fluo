[![Fluo][logo]][fluo.io]
---
[![Build Status][ti]][tl] [![Apache License][li]][ll] [![Maven Central][mi]][ml] [![Javadoc][ji]][jl]

**Fluo is transaction layer that enables incremental processing on big data.**

Fluo is an implementation of [Percolator] built on [Accumulo] that runs in [YARN].
It is not recommended for production use yet. Check out the Fluo [project website][fluo.io]
for news and general information.

### Getting Started

There are several ways to run Fluo (listed in order of increasing difficulty):

* [quickstart] - Starts a MiniFluo instance that is configured to run a word count application
* [MiniFluo] - Sets up a minimal Fluo instance that writes its data to single directory
* [fluo-dev] - Command-line tool for running Fluo and its dependencies on a single machine
* [Zetten] - Command-line tool that launches an AWS cluster and sets up Fluo/Accumulo on it
* [Production] - Sets up Fluo on a cluster where Accumulo, Hadoop & Zookeeper are running

Except for [quickstart], all above will set up a Fluo application that will be idle unless you
create client & observer code for your application.  You can either [create your own
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

[fluo.io]: http://fluo.io/
[Accumulo]: http://accumulo.apache.org
[Percolator]: http://research.google.com/pubs/pub36726.html
[YARN]: http://hadoop.apache.org/docs/r2.5.1/hadoop-yarn/hadoop-yarn-site/YARN.html
[quickstart]: https://github.com/fluo-io/fluo-quickstart
[fluo-dev]: https://github.com/fluo-io/fluo-dev
[Zetten]: https://github.com/fluo-io/zetten
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo-stress]: https://github.com/fluo-io/fluo-stress
[webindex]: https://github.com/fluo-io/webindex
[MiniFluo]: docs/mini-fluo-setup.md
[Production]: docs/prod-fluo-setup.md
[apps]: docs/applications.md
[api]: http://fluo.io/apidocs/
[recipes]: https://github.com/fluo-io/fluo-recipes
[Metrics]: docs/metrics.md
[Contributing]: docs/contributing.md
[Architecture]: docs/architecture.md
[ti]: https://travis-ci.org/fluo-io/fluo.svg?branch=master
[tl]: https://travis-ci.org/fluo-io/fluo
[li]: http://img.shields.io/badge/license-ASL-blue.svg
[ll]: https://github.com/fluo-io/fluo/blob/master/LICENSE
[mi]: https://maven-badges.herokuapp.com/maven-central/org.apache.fluo/fluo-api/badge.svg
[ml]: https://maven-badges.herokuapp.com/maven-central/org.apache.fluo/fluo-api/
[ji]: https://javadoc-emblem.rhcloud.com/doc/org.apache.fluo/fluo-api/badge.svg
[jl]: http://www.javadoc.io/doc/org.apache.fluo/fluo-api
[logo]: contrib/fluo-logo.png
