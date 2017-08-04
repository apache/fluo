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

# Fluo metrics in Grafana/InfluxDB

This document describes how to send Fluo metrics to [InfluxDB], a time series database, and make
them viewable in [Grafana], a visualization tool. If you want general information on metrics, see
the [Fluo metrics][2] documentation.

## Set up Grafana/InfluxDB on your own

Follow the instructions below to setup InfluxDB and Grafana.

1.  Follow the standard installation instructions for [InfluxDB] and [Grafana]. As for versions,
    the instructions below were written using InfluxDB v0.9.4.2 and Grafana v2.5.0.

2.  Add the following to your InfluxDB configuration to configure it accept metrics in Graphite
    format from Fluo. The configuration below contains templates that transform the Graphite
    metrics into a format that is usable in InfluxDB.

    ```
    [[graphite]]
      bind-address = ":2003"
      enabled = true
      database = "fluo_metrics"
      protocol = "tcp"
      consistency-level = "one"
      separator = "_"
      batch-size = 1000
      batch-pending = 5
      batch-timeout = "1s"
      templates = [
        "fluo.class.*.*.*.*.* ..app.host.measurement.observer.field",
        "fluo.class.*.*.*.* ..app.host.measurement.observer",
        "fluo.system.*.*.*.* ..app.host.measurement.field",
        "fluo.system.*.*.* ..app.host.measurement",
        "fluo.app.*.*.* ..host.measurement.field",
        "fluo.app.*.* ..host.measurement",
      ]
    ```

3. Fluo distributes a file called `fluo_metrics_setup.txt` that contains a list of commands that
   setup InfluxDB. These commands will configure an InfluxDB user, retention policies, and
   continuous queries that downsample data for the historical dashboard in Grafana. Run the command
   below to execute the commands in this file:

    ```
    $INFLUXDB_HOME/bin/influx -import -path $FLUO_HOME/contrib/influxdb/fluo_metrics_setup.txt
    ```

3. Configure the `fluo-app.properties` of your Fluo application to send Graphite metrics to InfluxDB.
   Below is example configuration. Remember to replace `<INFLUXDB_HOST>` with the actual host.

    ```
    fluo.metrics.reporter.graphite.enable=true
    fluo.metrics.reporter.graphite.host=<INFLUXDB_HOST>
    fluo.metrics.reporter.graphite.port=2003
    fluo.metrics.reporter.graphite.frequency=30
    ```

    The reporting frequency of 30 sec is required if you are using the provided Grafana dashboards
    that are configured in the next step.

4.  Grafana needs to be configured to load dashboard JSON templates from a directory. Fluo
    distributes two Grafana dashboard templates in its tarball distribution in the directory
    `contrib/grafana`. Before restarting Grafana, you should copy the templates from your Fluo
    installation to the `dashboards/` directory configured below.

    ```
    [dashboards.json]
    enabled = true
    path = <GRAFANA_HOME>/dashboards
    ```

5.  If you restart Grafana, you will see the Fluo dashboards configured but all of their charts will
    be empty unless you have a Fluo application running and configured to send data to InfluxDB.
    When you start sending data, you may need to refresh the dashboard page in the browser to start
    viewing metrics.

[1]: https://dropwizard.github.io/metrics/3.1.0/
[2]: metrics.md
[Grafana]: http://grafana.org/
[InfluxDB]: https://influxdb.com/
