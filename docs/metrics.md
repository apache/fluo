Fluo Metrics
============

Fluo core is instrumented using [dropwizard metrics][1].  This allows fluo
users to easily gather information about Fluo by configuring different
reporters.  While dropwizard can be configured to report Fluo metrics to many
different tools, below are some tools that have been used with Fluo.

1. [Grafana/InfluxDB][3] - Fluo has [documentation][3] for sending metrics to
   InfluxDB and viewing them in Grafana. The [fluo-dev] tool can also set up
   these tools for you and configure Fluo to send to them.

2. JMX - Fluo can be configured to reports metrics via JMX which can be viewed
   in jconsole or jvisualvm.  
   
3. CSV - Fluo can be configured to output metrics as CSV to a specified directory.

Configuring Reporters
---------------------

Inorder to configure metrics reporters, look at the metrics section in an
applications `fluo.properties` file.  This sections has a lot of commented out
options for configuring reporters.  

    io.fluo.metrics.reporter.console.enable=false
    io.fluo.metrics.reporter.console.frequency=30

The frequency is in seconds for all reporters.
  
Metrics reported by Fluo
------------------------

All metrics reported by Fluo have the prefix `io.fluo.<APP>.<PID>.` which is denoted by `<prefix>` in
the table below.  In the prefix, `<APP>` represents the Fluo application name and `<PID>` is the 
process ID of the Fluo oracle or worker that is reporting the metric.  When running in yarn, this 
id is of the format `worker-<instance id>` or `oracle-<instance id>`.  When not running from yarn, 
this id consist of a hostname and a base36 long that is unique across all fluo processes. 

Some of the metrics reported have the class name as the suffix.  This classname
is the observer or load task that executed the transactions.   This should
allow things like transaction collisions to be tracked per class.  In the
table below this is denoted with `<cn>`.  

|Metric                                 | Type           | Description                         |
|---------------------------------------|----------------|-------------------------------------|
|\<prefix\>.tx.lock_wait_time.\<cn\>    | [Timer][T]     | *WHEN:* After each transaction. *COND:* &gt; 0 *WHAT:* Time transaction spent waiting on locks held by other transactions.   |
|\<prefix\>.tx.execution_time.\<cn\>    | [Timer][T]     | *WHEN:* After each transaction. *WHAT:* Time transaction took to execute.  Updated for failed and successful transactions.  This does not include commit time, only the time from start until commit is called. |
|\<prefix\>.tx.with_collision.\<cn\>    | [Meter][M]     | *WHEN:* After each transaction. *WHAT:* Rate of transactions with collisions.  |
|\<prefix\>.tx.collisions.\<cn\>        | [Meter][M]     | *WHEN:* After each transaction. *WHAT:* Rate of collisions.  |
|\<prefix\>.tx.entries_set.\<cn\>       | [Meter][H]     | *WHEN:* After each transaction. *WHAT:* Rate of row/columns set by transaction |
|\<prefix\>.tx.entries_read.\<cn\>      | [Meter][H]     | *WHEN:* After each transaction. *WHAT:* Rate of row/columns read by transaction that existed.  There is currently no count of all reads (including non-existent data) |
|\<prefix\>.tx.locks_timedout.\<cn\>    | [Meter][M]     | *WHEN:* After each transaction. *WHAT:* Rate of timedout locks rolled back by transaction.  These are locks that are held for very long periods by another transaction that appears to be alive based on zookeeper.  |
|\<prefix\>.tx.locks_dead.\<cn\>        | [Meter][M]     | *WHEN:* After each transaction. *WHAT:* Rate of dead locks rolled by a transaction.  These are locks held by a process that appears to be dead according to zookeeper.  |
|\<prefix\>.tx.status_\<status\>.\<cn\> | [Meter][M]     | *WHEN:* After each transaction. *WHAT:* Rate of different ways a transaction can terminate |
|\<prefix\>.oracle.response_time        | [Timer][T]     | *WHEN:* For each request for stamps to the server. *WHAT:* Time RPC call to oracle took |
|\<prefix\>.oracle.client_stamps        | [Histogram][H] | *WHEN:* For each request for stamps to the server. *WHAT:* The number of stamps requested. |
|\<prefix\>.oracle.server_stamps        | [Histogram][H] | *WHEN:* For each request for stamps from a client. *WHAT:* The number of stamps requested. |
|\<prefix\>.worker.notifications_queued | [Gauge][G]     | *WHAT:* The current number of notifications queued for processing. |
|\<prefix\>.transactor.committing       | [Gauge][G]     | *WHAT:* The current number of transactions that are working their way through the commit steps. |

The table above outlines when a particular metric is updated and whats updated.
The use of *COND* indicates that the metric is not always updated.   For
example `i.f.<pid>.tx.lockWait.<cn>` is only updated for transactions that had a non
zero lock wait time.  

Histograms and Timers have a counter.  In the case of a histogram, the counter
is the number of times the metric was updated and not a sum of the updates.
For example if a request for 5 timestamps was made to the oracle followed by a
request for 3 timestamps, then the count for `i.f.<pid>.oracle.server.stamps` would
be 2 and the mean would be (5+3)/2.

[1]: https://dropwizard.github.io/metrics/3.1.0/
[3]: grafana.md
[T]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#timers
[C]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#counters
[H]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#histograms
[G]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#gauges
[M]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#meters
[fluo-dev]: https://github.com/fluo-io/fluo-dev
