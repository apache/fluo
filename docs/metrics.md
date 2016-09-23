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

# Fluo Metrics

A Fluo application can be configured (in [fluo.properties]) to report metrics. When metrics are
configured, Fluo will report some 'default' metrics about an application that help users monitor its
performance. Users can also write code to report 'application-specific' metrics from their
applications. Both 'application-specific' and 'default' metrics share the same reporter configured
by [fluo.properties] and are described in detail below.

## Configuring reporters

Fluo metrics are not published by default. To publish metrics, configure a reporter in the 'metrics'
section of [fluo.properties]. There are several different reporter types (i.e Console, CSV,
Graphite, JMX, SLF4J) that are implemented using [Dropwizard]. The choice of which reporter to use
depends on the visualization tool used. If you are not currently using a visualization tool, there
is [documentation][grafana] for reporting Fluo metrics to Grafana/InfluxDB.

## Metrics names

When Fluo metrics are reported, they are published using a naming scheme that encodes additional
information. This additional information is represented using all caps variables (i.e `METRIC`)
below.

Default metrics start with `fluo.class` or `fluo.system` and have following naming schemes:

        fluo.class.APPLICATION.REPORTER_ID.METRIC.CLASS
        fluo.system.APPLICATION.REPORTER_ID.METRIC

Application metrics start with `fluo.app` and have following scheme:

        fluo.app.REPORTER_ID.METRIC

The variables below describe the additional information that is encoded in metrics names.

1. `APPLICATION` - Fluo application name
2. `REPORTER_ID` - Unique ID of the Fluo oracle, worker, or client that is reporting the metric.
    When running in YARN, this ID is of the format `worker-INSTANCE_ID` or `oracle-INSTANCE_ID`
    where `INSTANCE_ID` corresponds to instance number. When not running in YARN, this ID consists
    of a hostname and a base36 long that is unique across all fluo processes.
3. `METRIC` - Name of the metric. For 'default' metrics, this is set by Fluo. For 'application'
    metrics, this is set by user. Name should be unique and avoid using period '.' in name.
4. `CLASS` - Name of Fluo observer or loader class that produced metric. This allows things like
    transaction collisions to be tracked per class.

## Application-specific metrics

Application metrics are implemented by retrieving a [MetricsReporter] from an [Observer], [Loader],
or [FluoClient].  These metrics are named using the format `fluo.app.REPORTER_ID.METRIC`.

## Default metrics

Default metrics report for a particular Observer/Loader class or system-wide.

Below are metrics that are reported from each Observer/Loader class that is configured in a Fluo
application. These metrics are reported after each transaction and named using the format
`fluo.class.APPLICATION.REPORTER_ID.METRIC.CLASS`.

* tx_lock_wait_time - [Timer]
    - Time transaction spent waiting on locks held by other transactions.
    - Only updated for transactions that have non-zero lock time.
* tx_execution_time - [Timer]
    - Time transaction took to execute.
    - Updated for failed and successful transactions.
    - This does not include commit time, only the time from start until commit is called.
* tx_with_collision - [Meter]
    - Rate of transactions with collisions.
* tx_collisions - [Meter]
    - Rate of collisions.
* tx_entries_set - [Meter]
    - Rate of row/columns set by transaction
* tx_entries_read - [Meter]
    - Rate of row/columns read by transaction that existed.
    - There is currently no count of all reads (including non-existent data)
* tx_locks_timedout - [Meter]
    - Rate of timedout locks rolled back by transaction.
    - These are locks that are held for very long periods by another transaction that appears to be
      alive based on zookeeper.
* tx_locks_dead - [Meter]
    - Rate of dead locks rolled by a transaction.
    - These are locks held by a process that appears to be dead according to zookeeper.
* tx_status_`<STATUS>` - [Meter]
    - Rate of different ways (i.e `<STATUS>`) a transaction can terminate

Below are system-wide metrics that are reported for the entire Fluo application. These metrics are
named using the format `fluo.system.APPLICATION.REPORTER_ID.METRIC`.

* oracle_response_time - [Timer]
    - Time each RPC call to oracle for stamps took
* oracle_client_stamps - [Histogram]
    - Number of stamps requested for each request for stamps to the server
* oracle_server_stamps - [Histogram]
    - Number of stamps requested for each request for stamps from a client
* worker_notifications_queued - [Gauge]
    - The current number of notifications queued for processing.
* transactor_committing - [Gauge]
    - The current number of transactions that are working their way through the commit steps.

Histograms and Timers have a counter. In the case of a histogram, the counter is the number of times
the metric was updated and not a sum of the updates. For example if a request for 5 timestamps was
made to the oracle followed by a request for 3 timestamps, then the count for `oracle_server_stamps`
would be 2 and the mean would be (5+3)/2.

[fluo.properties]: ../modules/distribution/src/main/config/fluo.properties
[Dropwizard]: https://dropwizard.github.io/metrics/3.1.0/
[grafana]: grafana.md
[MetricsReporter]: ../modules/api/src/main/java/org/apache/fluo/api/metrics/MetricsReporter.java
[Observer]: ../modules/api/src/main/java/org/apache/fluo/api/observer/Observer.java
[Loader]: ../modules/api/src/main/java/org/apache/fluo/api/client/Loader.java
[FluoClient]: ../modules/api/src/main/java/org/apache/fluo/api/client/FluoClient.java
[Timer]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#timers
[Counter]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#counters
[Histogram]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#histograms
[Gauge]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#gauges
[Meter]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#meters
