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

Instructions for installing Apache Fluo and starting a Fluo application on a cluster where
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

    git clone https://github.com/apache/fluo.git
    cd fluo/
    mvn package

## Install Fluo

After you obtain a Fluo distribution tarball, follow these steps to install Fluo.

1.  Choose a directory with plenty of space and untar the distribution:

        tar -xvzf fluo-1.1.0-incubating-bin.tar.gz
        cd fluo-1.1.0-incubating

    The distribution contains a `fluo` script in `bin/` that administers Fluo and the
    following configuration files in `conf/`:

    | Configuration file           | Description                                                                                  |
    |------------------------------|----------------------------------------------------------------------------------------------|
    | [fluo-env.sh]                | Configures classpath for `fluo` script. Required for all commands.                           |
    | [fluo-conn.properties]       | Configures connection to Fluo. Required for all commands.                                    |
    | [fluo-app.properties]        | Template for configuration file passed to `fluo init` when initializing Fluo application.    |
    | [log4j.properties]           | Configures logging                                                                           |
    | [fluo.properties.deprecated] | Deprecated Fluo configuration file. Replaced by fluo-conn.properties and fluo-app.properties |

2.  Configure [fluo-env.sh] to set up your classpath using jars from the versions of Hadoop, Accumulo, and
Zookeeper that you are using. Choose one of the two ways below to make these jars available to Fluo:

    * Set `HADOOP_PREFIX`, `ACCUMULO_HOME`, and `ZOOKEEPER_HOME` in your environment or configure
    these variables in [fluo-env.sh]. Fluo will look in these locations for jars.
    * Run `./lib/fetch.sh ahz` to download Hadoop, Accumulo, and Zookeeper jars to `lib/ahz` and
    configure [fluo-env.sh] to look in this directory. By default, this command will download the
    default versions set in [lib/ahz/pom.xml]. If you are not using the default versions, you can
    override them:

            ./lib/fetch.sh ahz -Daccumulo.version=1.7.2 -Dhadoop.version=2.7.2 -Dzookeeper.version=3.4.8

3. Fluo needs more dependencies than what is available from Hadoop, Accumulo, and Zookeeper. These
   extra dependencies need to be downloaded to `lib/` using the command below:

        ./lib/fetch.sh extra

You are now ready to use the `fluo` script.

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

## Initialize Fluo application

1. Create a copy of [fluo-app.properties] for your Fluo application. 

        cp conf/fluo-app.properties /path/to/myapp/fluo-app.properties

2. Edit your copy of [fluo-app.properties] and make sure to set the following:

    * Class name of your ObserverProvider
    * Paths to your Fluo observer jars
    * Accumulo configuration
    * HDFS configuration

   When configuring the observer section of fluo-app.properties, you can configure your instance for the
   [phrasecount] application if you have not created your own application. See the [phrasecount]
   example for instructions. You can also choose not to configure any observers but your workers will
   be idle when started.

3. Run the command below to initialize your Fluo application. Change `myapp` to your application name:

        fluo init myapp /path/to/myapp/fluo-app.properties

   A Fluo application only needs to be initialized once. After initialization, the Fluo application
   name is used to start/stop the application and scan the Fluo table.

4. Run `fluo list` which connects to Fluo and lists applications to verify initialization.

## Start your Fluo application

Follow the instructions below to start a Fluo application which contains an oracle and multiple workers.

1. Configure [fluo-env.sh] and [fluo-conn.properties] if you have not already.

2. Run Fluo application processes using the `fluo oracle` and `fluo worker` commands. Fluo applications
   are typically run with one oracle process and multiple worker processes. The commands below will start
   a Fluo oracle and two workers on your local machine:

        fluo oracle myapp
        fluo worker myapp
        fluo worker myapp

   The commands will retrieve your application configuration and observer jars (using your
   application name) before starting the oracle or worker process.

The oracle & worker logs can be found in the directory `logs/<applicationName>` of your Fluo installation.

If you want to distribute the processes of your Fluo application across a cluster, you will need install
Fluo on every node where you want to run a Fluo process and follow the instructions above on each node.

## Manage your Fluo application

When you have data in your Fluo application, you can view it using the command `fluo scan myapp`. 
Pipe the output to `less` using the command `fluo scan myapp | less` if you want to page through the data.

To list all Fluo applications, run `fluo list`.

To stop your Fluo application, run `jps -m | grep Fluo` to find process IDs and use `kill` to stop them.

## Tuning Accumulo

Fluo will reread the same data frequently when it checks conditions on mutations. When Fluo
initializes a table it enables data caching to make this more efficient. However you may need to
increase the amount of memory available for caching in the tserver by increasing
`tserver.cache.data.size`. Increasing this may require increasing the maximum tserver java heap size
in `accumulo-env.sh`.

Fluo will run many client threads, will want to ensure the tablet server has enough threads. Should
probably increase the `tserver.server.threads.minimum` Accumulo setting.

Using at least Accumulo 1.6.1 is recommended because multiple performance bugs were fixed.

[Accumulo]: https://accumulo.apache.org/
[Hadoop]: http://hadoop.apache.org/
[Zookeeper]: http://zookeeper.apache.org/
[Java]: http://openjdk.java.net/
[related]: https://fluo.apache.org/related-projects/
[release]: https://fluo.apache.org/download/
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo-conn.properties]: ../modules/distribution/src/main/config/fluo-conn.properties
[fluo-app.properties]: ../modules/distribution/src/main/config/fluo-app.properties
[log4j.properties]: ../modules/distribution/src/main/config/log4j.properties
[fluo.properties.deprecated]: ../modules/distribution/src/main/config/fluo.properties.deprecated
[fluo-env.sh]: ../modules/distribution/src/main/config/fluo-env.sh
[lib/ahz/pom.xml]: ../modules/distribution/src/main/lib/ahz/pom.xml
