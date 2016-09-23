<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

[![Fluo][logo]][fluo]
---
[![Build Status][ti]][tl] [![Apache License][li]][ll] [![Maven Central][mi]][ml] [![Javadoc][ji]][jl]

**Apache Fluo lets users make incremental updates to large data sets stored in Apache Accumulo.**

[Apache Fluo][fluo] is an open source implementation of [Percolator][percolator] (which populates
Google's search index) for [Apache Accumulo][accumulo]. Fluo makes it possible to update the results
of a large-scale computation, index, or analytic as new data is discovered. Check out the Fluo
[project website][fluo] for news and general information.

## Getting Started

* Take the [Fluo Tour][tour] if you are completely new to Fluo.
* Read the [install instructions][install] to install Fluo and start a Fluo application in YARN on a
  cluster where Accumulo, Hadoop & Zookeeper are running. If you need help setting up these
  dependencies, see the [related projects page][related] for external projects that may help.

## Applications

Below are helpful resources for Fluo application developers:

*  [Instructions][apps] for creating Fluo applications
*  [Fluo API][api] javadocs
*  [Fluo Recipes][recipes] is a project that provides common code for Fluo application developers
   implemented using the Fluo API.

## Implementation

*  [Architecture] - Overview of Fluo's architecture
*  [Contributing] - Documentation for developers who want to contribute to Fluo
*  [Metrics] - Fluo metrics are visible via JMX by default but can be configured to send to Graphite
   or Ganglia

[fluo]: https://fluo.apache.org/
[related]: https://fluo.apache.org/related-projects/
[tour]: https://fluo.apache.org/tour/
[accumulo]: https://accumulo.apache.org
[percolator]: https://research.google.com/pubs/pub36726.html
[install]: docs/install.md
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
