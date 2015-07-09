Fluo Metrics
============

Fluo core is instrumented using [dropwizard metrics][1].  This allows fluo
users to easily gather information about Fluo by configuring different
reporters.  Fluo will always setup a JMX reporter, regardless of the number of
reporters configured.  This is done because the dropwizard config mechanism does
not currently support the JMX reporter.   The JMX reporter makes it easy to see
fluo stats in jconsole or jvisualvm.

Configuring Fluo processes
--------------------------

When starting an oracle or workers, using the `fluo` script, the
`$FLUO_CONF_DIR/metrics.yaml` file is used to configure reporters.  Consult the
[dropwizard config docs][2] inorder to learn how to populate this file.  There
is one important difference with that documentation. Because Fluo is only
leveraging the dropwizard metrics config code, you do not need the top level
`metrics:` element in your `metrics.yaml` file.  The example `metrics.yaml`
file does not have this element.

Configuring Fluo Clients
------------------------

Fluo client code that uses the basic API or map reduce API can configure
reporters by setting `io.fluo.metrics.yaml.base64` in `fluo.properties`.  The
value of this property should be a single line base64 encoded yaml config.
This can easily be generated with the following command.  Also,
FluoConfiguration has some convenience methods for setting this property.

```
cat conf/metrics.yaml | base64 -w 0
```  

The property `io.fluo.metrics.yaml.base64` is not used by processes started
with the fluo script.  The primary motivation of having this property is to
enable collection of metrics from map task executing load transaction using 
FluoOutputFormat.

In order for the `io.fluo.metrics.yaml.base64` property to work, a map reduce
job must include the `fluo-metrics` module.  This module contains the code that
configures reporters based on yaml.  The module is separate from `fluo-core`
inorder to avoid adding a lot of dependencies that are only needed when
configuring reporters.

Reporter Dependencies
---------------------

The core dropwizard metrics library has a few reporters.  However if you would
like to utilize additional reporters, then you will need to add dependencies.
For example if you wanted to use Ganglia, then you would need to depend on
specific dropwizard ganglia maven artifacts.

Custom Reporters
----------------

If a reporter follows the discovery mechanisms used by dropwizard
configuration, then it may be automatically configurable via yaml.  However
this has not been tested.

Metrics reported by Fluo
------------------------

Some of the metrics reported have the class name as the suffix.  This classname
is the observer or load task that executed the transactions.   This should
allow things like transaction collisions to be tracked per class.  In the
table below this is denoted with `<cn>`.  In the table below `io.fluo` is
shortened to `i.f`.  

Since multiple processes can report the same metrics to services like Graphite
or Ganglia, each process adds a unique id.  When running in yarn, this id is of
the format `worker-<instance id>` or `oracle-<instance id>`.  When not running
from yarn, this id consist of a hostname and a base36 long that is unique across
all fluo processes.  In the table below this composite id is represented with
`<pid>`. 

|Metric                                   | Type           | Description                         |
|-----------------------------------------|----------------|-------------------------------------|
|i.f.&lt;pid&gt;.tx.lockWait.&lt;cn&gt;               | [Timer][T]     | *WHEN:* After each transaction. *COND:* &gt; 0 *WHAT:* Time transaction spent waiting on locks held by other transactions.   |
|i.f.&lt;pid&gt;.tx.time.&lt;cn&gt;                   | [Timer][T]     | *WHEN:* After each transaction. *WHAT:* Time transaction took to execute.  Updated for failed and successful transactions. |
|i.f.&lt;pid&gt;.tx.collisions.&lt;cn&gt;             | [Histogram][H] | *WHEN:* After each transaction. *COND:* &gt; 0 *WHAT:* Number of collisions a transaction had.  |
|i.f.&lt;pid&gt;.tx.set.&lt;cn&gt;                    | [Histogram][H] | *WHEN:* After each transaction. *WHAT:* Number of row/columns set by transaction |
|i.f.&lt;pid&gt;.tx.read.&lt;cn&gt;                   | [Histogram][H] | *WHEN:* After each transaction. *WHAT:* Number of row/columns read by transaction that existed.  There is currently no count of all reads (including non-existent data) |
|i.f.&lt;pid&gt;.tx.locks.timedout.&lt;cn&gt;         | [Histogram][H] | *WHEN:* After each transaction. *COND:* &gt; 0 *WHAT:* Number of timedout locks rolled back by transaction.  These are locks that are held for very long periods by another transaction that appears to be alive based on zookeeper.  |
|i.f.&lt;pid&gt;.tx.locks.dead.&lt;cn&gt;             | [Histogram][H] | *WHEN:* After each transaction. *COND:* &gt; 0 *WHAT:* Number of dead locks rolled by a transaction.  These are locks held by a process that appears to be dead according to zookeeper.  |
|i.f.&lt;pid&gt;.tx.status.&lt;status&gt;.&lt;cn&gt;  | [Counter][C]   | *WHEN:* After each transaction.  *WHAT:* Counts for the different ways a transaction can terminate |
|i.f.&lt;pid&gt;.oracle.client.rpc.getStamps.time     | [Timer][T]     | *WHEN:* For each request for stamps to the server. *WHAT:* Time RPC call to oracle took |
|i.f.&lt;pid&gt;.oracle.client.stamps                 | [Histogram][H] | *WHEN:* For each request for stamps to the server. *WHAT:*  The number of stamps requested. |
|i.f.&lt;pid&gt;.oracle.server.stamps                 | [Histogram][H] | *WHEN:* For each request for stamps from a client.  *WHAT:* The number of stamps requested    |
|i.f.&lt;pid&gt;.worker.notifications.queued          | [Gauge][G]     | *WHAT:* The current number of notifications queued for processing    |

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
[2]: https://dropwizard.github.io/dropwizard/manual/configuration.html#metrics
[T]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#timers
[C]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#counters
[H]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#histograms
[G]: https://dropwizard.github.io/metrics/3.1.0/getting-started/#gauges
