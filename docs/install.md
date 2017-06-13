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

# Fluo Install Instructions

Instructions for installing Apache Fluo and starting a Fluo application in YARN on a cluster where
Accumulo, Hadoop & Zookeeper are running.  If you need help setting up these dependencies, see the
[related projects page][related] for external projects that may help.

## Requirements

Before you install Fluo, the following software must be installed and running on your local machine
or cluster:

| Software    | Recommended Version | Minimum Version |
|-------------|---------------------|-----------------|
| [Accumulo]  | 1.7.2               | 1.6.1           |
| [Hadoop]    | 2.7.2               | 2.6.0           |
| [Zookeeper] | 3.4.8               |                 |
| [Java]      | JDK 8               | JDK 8           |

## Obtain a distribution

Before you can install Fluo, you will need to obtain a distribution tarball. It is recommended that
you download the [latest release][release]. You can also build a distribution from the master
branch by following these steps which create a tarball in `modules/distribution/target`:

    git clone https://github.com/apache/incubator-fluo.git
    cd fluo/
    mvn package

## Install Fluo

After you obtain a Fluo distribution tarball, follow these steps to install Fluo.

1.  Choose a directory with plenty of space and untar the distribution:

        tar -xvzf fluo-1.1.0-incubating-bin.tar.gz

2.  Copy the example configuration to the base of your configuration directory to create the default
    configuration for your Fluo install:

        cp conf/examples/* conf/

    The default configuration will be used as the base configuration for each new application.

3.  Modify [fluo.properties] for your environment. However, you should not configure any
    application settings (like observers).

    NOTE - All properties that have a default are set with it. Uncomment a property if you want
    to use a value different than the default. Properties that are unset and uncommented must be
    set by the user.

4. Fluo needs to build its classpath using jars from the versions of Hadoop, Accumulo, and
Zookeeper that you are using. Choose one of the two ways below to make these jars available
to Fluo:

    * Set `HADOOP_PREFIX`, `ACCUMULO_HOME`, and `ZOOKEEPER_HOME` in your environment or configure
    these variables in [fluo-env.sh]. Fluo will look in these locations for jars.
    * Run `./lib/fetch.sh ahz` to download Hadoop, Accumulo, and Zookeeper jars to `lib/ahz` and
    configure [fluo-env.sh] to look in this directory. By default, this command will download the
    default versions set in [lib/ahz/pom.xml]. If you are not using the default versions, you can
    override them:

            ./lib/fetch.sh ahz -Daccumulo.version=1.7.2 -Dhadoop.version=2.7.2 -Dzookeeper.version=3.4.8

5. Fluo needs more dependencies than what is available from Hadoop, Accumulo, and Zookeeper. These
   extra dependencies need to be downloaded to `lib/` using the command below:

        ./lib/fetch.sh extra

You are now ready to use the Fluo command script.

## Fluo command script

The Fluo command script is located at `bin/fluo` of your Fluo installation. All Fluo commands are
invoked by this script.

Modify and add the following to your `~/.bashrc` if you want to be able to execute the fluo script
from any directory:

    export PATH=/path/to/fluo-1.1.0-incubating/bin:$PATH

Source your `.bashrc` for the changes to take effect and test the script

    source ~/.bashrc
    fluo

Running the script without any arguments prints a description of all commands.

    ./bin/fluo

## Configure a Fluo application

You are now ready to configure a Fluo application. Use the command below to create the
configuration necessary for a new application. Feel free to pick a different name (other than
`myapp`) for your application:

    fluo new myapp

This command will create a directory for your application at `apps/myapp` of your Fluo install which
will contain a `conf` and `lib`.

The `apps/myapp/conf` directory contains a copy of the `fluo.properties` from your default
configuration. This should be configured for your application:

    vim apps/myapp/fluo.properties

When configuring the observer section in fluo.properties, you can configure your instance for the
[phrasecount] application if you have not created your own application. See the [phrasecount]
example for instructions. You can also choose not to configure any observers but your workers will
be idle when started.

The `apps/myapp/lib` directory should contain any observer jars for your application. If you
configured [fluo.properties] for observers, copy any jars containing these observer classes to this
directory.

## Initialize your application

After your application has been configured, use the command below to initialize it:

    fluo init myapp

This only needs to be called once and stores configuration in Zookeeper.

## Start your application

A Fluo application consists of one oracle process and multiple worker processes. Before starting
your application, you can configure the number of worker process in your [fluo.properties] file.

When you are ready to start your Fluo application on your YARN cluster, run the command below:

    fluo start myapp

The start command above will work for a single-node or a large cluster. By using YARN, you do not
need to deploy the Fluo binaries to every node on your cluster or start processes on every node.

You can use the following command to check the status of your instance:

    fluo status myapp

For more detailed information on the YARN containers running Fluo:

    fluo info myapp

You can also use `yarn application -list` to check the status of your Fluo instance in YARN. 

## View Fluo application logs

Fluo application logs are viewable within YARN using the methods below:

1. View logs using the web interface of the the YARN resource manager
(`http://<resource manager>:8088/cluster`). First, click on the application ID (i.e `application_*`)
of your Fluo application and then click on the latest attempt ID (i.e `appattempt_*`). You should
see a list of containers. There should be a container for the application master (typically
container 1), a Fluo oracle (typically container 2), and Fluo workers (containers 3+). You can view
the log files produced by a container by clicking on its 'logs' link. Logs from Fluo observers will
be in the `worker_*.log` file for each of your worker containers. 

2. View logs on disk in the directory specified by the YARN property `yarn.nodemanager.log-dirs` in
your YARN configuration `yarn-site.xml` (see [yarn-default.xml] for more info about this property).
If you are running Fluo on single machine, this method works well. If you are running Fluo on a
cluster, your containers and their logs will be spread across the cluster.

When running Fluo in YARN, Fluo must use [logback] for logging (due to a hard requirement by Twill).
Logback is configured using `/path/to/fluo/apps/<app_name>/conf/logback.xml`. You should review this
configuration but the root logger is configured by default to print any message that is debug level
or higher.

In addition the `*.log` files, Fluo oracle and worker containers will have `stdout` and `stderr`
files. These files may have helpful error messages. Especially, if a process failed to start
or calls were made to `System.out` or `System.err` in your application.

## View Fluo application data

When you have data in your Fluo application, you can view it using the command `fluo scan`. Pipe the
output to `less` using the command `fluo scan | less` if you want to page through the data.

## Stop your Fluo application

Use the following command to stop your Fluo application:

    fluo stop myapp

If stop fails, there is also a kill command.

    fluo kill myapp

## Tuning Accumulo

Fluo will reread the same data frequently when it checks conditions on mutations. When Fluo
initializes a table it enables data caching to make this more efficient. However you may need to
increase the amount of memory available for caching in the tserver by increasing
`tserver.cache.data.size`. Increasing this may require increasing the maximum tserver java heap size
in `accumulo-env.sh`.

Fluo will run many client threads, will want to ensure the tablet server has enough threads. Should
probably increase the `tserver.server.threads.minimum` Accumulo setting.

Using at least Accumulo 1.6.1 is recommended because multiple performance bugs were fixed.

## Tuning YARN

When running Fluo oracles and workers in YARN, the number of instances, max memory, and number of
cores for Fluo processes can be configured in [fluo.properties]. If YARN is killing processes
consider increasing `twill.java.reserved.memory.mb` (which defaults to 200 and is set in
yarn-site.xml). The `twill.java.reserved.memory.mb` config determines the gap between the YARN
memory limit set in [fluo.properties] and the java -Xmx setting. For example, if max memory is 1024
and twill reserved memory is 200, the java -Xmx setting will be 1024-200 = 824 MB.

## Run locally without YARN

If you do not have YARN set up, you can start the oracle and worker as a local Fluo process using
the following commands:

    local-fluo start-oracle
    local-fluo start-worker

Use the following commands to stop a local Fluo process:

    local-fluo stop-worker
    local-fluo stop-oracle

In a distributed environment, you will need to deploy and configure a Fluo distribution on every
node in your cluster.

[Accumulo]: https://accumulo.apache.org/
[Hadoop]: http://hadoop.apache.org/
[Zookeeper]: http://zookeeper.apache.org/
[Java]: http://openjdk.java.net/
[related]: https://fluo.apache.org/related-projects/
[release]: https://fluo.apache.org/download/
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo.properties]: ../modules/distribution/src/main/config/fluo.properties
[fluo-env.sh]: ../modules/distribution/src/main/config/fluo-env.sh
[lib/ahz/pom.xml]: ../modules/distribution/src/main/lib/ahz/pom.xml
[yarn-default.xml]: https://hadoop.apache.org/docs/r2.7.0/hadoop-yarn/hadoop-yarn-common/yarn-default.xml
[logback]: http://logback.qos.ch/
