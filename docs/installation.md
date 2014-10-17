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

Installation
============

Before you install Fluo, you will need the following installed and running on
your local machine or cluster:

* [Accumulo][Accumulo] (version 1.6+)
* [Hadoop][Hadoop] (version 2.2+)
* [Zookeeper]
* [Java][Java] (version 7+)

Obtaining a distribution
------------------------

Before you can install Fluo, you will need to obtain a distribution tarball that
works for your environment. Fluo distributions are built for specific releases
of Hadoop and Accumulo. If you are using Accumulo 1.6.1 and Hadoop 2.3.0,
you can download the [latest release][release].  If you need a release for a
different environment or one built from the master branch, follow these steps:

First, clone Fluo:
```
git clone https://github.com/fluo-io/fluo.git
cd fluo/
```

Optionally, check out a stable tag (if you don't want to build a release from master):
```
git checkout 1.0.0-alpha-1
```

Next, build a distribution for your environment. The tarball will be created in 
`modules/distribution/target`.
```
mvn package -Daccumulo.version=1.6.1 -Dhadoop.version=2.4.0
```

Installing and configuring Fluo
-------------------------------

When you have a distribution tarball built for your environment, follow these steps
to install and configure Fluo.

First, choose a directory with plenty of space and untar the distribution:
```
tar -xvzf fluo-1.0.0-beta-1-SNAPSHOT-bin.tar.gz
```

Verify that your distribution has the same versions of Hadoop & Accumulo as your environment:
```
cd fluo-1.0.0-beta-1-SNAPSHOT
ls lib/hadoop-* lib/accumulo-*
```

Next, copy the example configuration to the base of your configuration directory:
```
cp conf/examples/* conf/
```

Modify [fluo.properties] for your environment. NOTE - All properties that have a 
default are set with it.  Uncomment a property if you want to use a value different 
than the default.  Properties that are unset and uncommented must be set by the user.
When configuring the observer section in fluo.properties, you can configure your instance
for the [phrasecount] application if you have not created your own application. See
the [phrasecount] example for instructions. You can also choose not to configure any
observers but your workers will be idle when started.
```
vim conf/fluo.properties
```

Finally, if you configured [fluo.properties] for observers, copy any jars containing these
observer classes to `lib/observers` of your Fluo installation.

Fluo command script
-------------------

The Fluo command script is located at `bin/fluo` of your Fluo installation.  All Fluo
commands are invoked by this script.  

Modify and add the following to your `~/.bashrc` if you want to be able to execute the
fluo script from any directory:
```
export PATH=/path/to/fluo-1.0.0-beta-1-SNAPSHOT/bin:$PATH
```

Source your `.bashrc` for the changes to take effect and test the script
```
source ~/.bashrc
fluo
```
Running the script without any arguments prints a description of all commands.

Initializing Fluo
-----------------

After Fluo is installed and configured, initialize your instance:
```
fluo init
```
This only needs to be called once and stores configuration in Zookeeper.

Running Fluo
------------

A Fluo instance consists of one oracle process and multiple worker processes.
These processes can either be run on a YARN cluster or started locally on each
machine.

The preferred method to run a Fluo instance is using YARN which will start
up multiple workers as configured in [fluo.properties].  To start a Fluo cluster 
in YARN, run following command:
```
fluo yarn start
```

The start command above will work for a single-node or a large cluster.  By
using YARN, you no longer need to deploy the Fluo binaries to every node on your
cluster or start processes on every node.

You can use the following command to check the status of your instance:
```
fluo yarn status
```
You can also use `yarn application -list` to check the status of your Fluo instance
in YARN.  Logs are viewable within YARN.  

If you do not have YARN set up, you can start a local Fluo process using
the following commands:
```
fluo local start-oracle
fluo local start-worker
```

In a distributed environment, you will need to deploy the Fluo binary to 
every node and start each process individually.

Stopping Fluo
-------------

If you are using YARN, use the following command to stop your Fluo instance:
```
fluo yarn stop
```
If stop fails, there is also a kill command.
```
fluo yarn kill
```

If you are not using YARN, use the following commands to stop a local Fluo
process.  In a distributed environment, these command will need to be run
on every machine where processes are running:
```
fluo local stop-worker
fluo local stop-oracle
```

Tuning Accumulo
---------------

Fluo will reread the same data frequently when it checks conditions on
mutations.   When Fluo initializes a table it enables data caching to make
this more efficient.  However you may need to increase the amount of memory
available for caching in the tserver by increasing `tserver.cache.data.size`.
Increasing this may require increasing the maximum tserver java heap size in
`accumulo-env.sh`.  

Fluo will run many client threads, will want to ensure the tablet server
has enough threads.  Should probably increase the
`tserver.server.threads.minimum` Accumulo setting.

Using at least Accumulo 1.6.1 is recommended because multiple performance bugs
were fixed.

[Accumulo]: https://accumulo.apache.org/
[Hadoop]: http://hadoop.apache.org/
[Zookeeper]: http://zookeeper.apache.org/
[Java]: https://www.oracle.com/java/index.html
[release]: https://github.com/fluo-io/fluo/releases
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo.properties]: modules/distribution/src/main/config/fluo.properties
