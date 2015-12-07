
# Fluo metrics in Grafana/InfluxDB

Fluo is instrumented using [dropwizard metrics][1] which allows Fluo to be configured
to send metrics to multiple metrics tools (such as Graphite, Ganglia, etc).  

This document describes how to send Fluo metrics to [InfluxDB], a time series database, and make 
them viewable in [Grafana], a visualization tool.  If you want general information on metrics, see the 
[Fluo metrics][2] documentation. 

## Set up Grafana/InfluxDB using fluo-dev

The easiest way to view the metrics coming from Fluo is to use [fluo-dev] which
can be configured to setup InfluxDB and Grafana as well have Fluo send data to
them.  Fluo-dev will also set up a Fluo dashboard in Grafana.

## Set up Grafana/InfluxDB on your own

If cannot use [fluo-dev], you can follow the instructions below to setup InfluxDB and 
Grafana on your own.

1.  Follow the standard installation instructions for [InfluxDB] and [Grafana].  As for versions, 
    the instructions below were written using InfluxDB v0.9.4.2 and Grafana v2.5.0. 

2.  Add the following to your InfluxDB configuration to configure it accept metrics
    in Graphite format from Fluo.  The configuration below contains templates that
    transform the Graphite metrics into a format that is usable in InfluxDB.

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
        "io.fluo.*.*.tx.*.*.* ..app.host.measurement.measurement.observer.field",
        "io.fluo.*.*.*.*.* ..app.host.measurement.measurement.field",
        "io.fluo.*.*.*.* ..app.host.measurement.measurement",
      ]
    ```

3. Configure `fluo.properties` in your Fluo app configuration to send Graphite 
   metrics to InfluxDB.  Below is example configuration. Remember to replace
   `<INFLUXDB_HOST>` with the actual host.

    ```
    io.fluo.metrics.reporter.graphite.enable=true
    io.fluo.metrics.reporter.graphite.host=<INFLUXDB_HOST>
    io.fluo.metrics.reporter.graphite.port=2003
    io.fluo.metrics.reporter.graphite.frequency=10
    ```

4.  Grafana needs to be configured to load dashboard JSON templates from a
    directory.  Fluo distributes a Grafana dashboard template in its tarball 
    distribution that is located in the directory `contrib/grafana`. Before
    restarting Grafana, you should copy the template from your Fluo installation
    to you the `dashboards/` directory configured below.

    ```
    [dashboards.json]
    enabled = true
    path = <GRAFANA_HOME>/dashboards
    ```

5.  If you restart Grafana, you will see the Fluo dashboard configured but all of its charts will 
    be empty unless you have a Fluo application running and configured to send
    data to InfluxDB.  When you start sending data, you may need to refresh the dashboard page in 
    the browser to start viewing metrics.

[1]: https://dropwizard.github.io/metrics/3.1.0/
[2]: metrics.md
[fluo-dev]: https://github.com/fluo-io/fluo-dev
[Grafana]: http://grafana.org/
[InfluxDB]: https://influxdb.com/
