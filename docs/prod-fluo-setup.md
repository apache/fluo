Production Setup
================

Below are instructions for running Fluo in a production environment where Accumulo,
Hadoop & Zookeeper are installed and running.  If you want to avoid setting up
these dependencies, consider using [fluo-dev]. 

Requirements
------------

Before you install Fluo, you will need the following installed and running on
your local machine or cluster:

* [Accumulo][Accumulo] (version 1.6+)
* [Hadoop][Hadoop] (version 2.2+)
* [Zookeeper]
* [Java][Java] (version 7+)

Obtain a distribution
---------------------

Before you can install Fluo, you will need to obtain a distribution tarball.  It is
recommended that you download the [latest release][release].  You can also build
a distribution from the master branch by following these steps which create a tarball
in `modules/distribution/target`:

    git clone https://github.com/apache/incubator-fluo.git
    cd fluo/
    mvn package

Install Fluo
------------

When you have a distribution tarball built for your environment, follow these steps
to install Fluo.

First, choose a directory with plenty of space and untar the distribution:

    tar -xvzf fluo-1.0.0-beta-2-bin.tar.gz

Verify that your distribution has the same versions of Hadoop & Accumulo as your environment:

    cd fluo-1.0.0-beta-2
    ls lib/hadoop-* lib/accumulo-*

Next, copy the example configuration to the base of your configuration directory to create
the default configuration for your Fluo install:

    cp conf/examples/* conf/

The default configuration will be used as the base configuration for each new application.
Therefore, you should modify [fluo.properties] for your environment.  However, you should not
configure any application settings (like observers). 

NOTE - All properties that have a default are set with it.  Uncomment a property if you want 
to use a value different than the default.  Properties that are unset and uncommented must be
set by the user.

To run, Fluo requires the Hadoop, Accumulo, and Zookeeper jars for the versions
running on the cluster where Fluo will run.  In order to configure this a
classpath must be constructed with these jars for Fluo.  This can be done by
editing `conf/fluo-env.sh`, which also contains further documentation.  

Fluo command script
-------------------

The Fluo command script is located at `bin/fluo` of your Fluo installation.  All Fluo
commands are invoked by this script.

Modify and add the following to your `~/.bashrc` if you want to be able to execute the
fluo script from any directory:

    export PATH=/path/to/fluo-1.0.0-beta-2/bin:$PATH

Source your `.bashrc` for the changes to take effect and test the script

    source ~/.bashrc
    fluo

Running the script without any arguments prints a description of all commands.

    ./bin/fluo

Configure a Fluo application
----------------------------

You are now ready to configure a Fluo application.  Use the command below to create the
configuration necessary for a new application.  Feel free to pick a different name (other
than `myapp`) for your application:

    fluo new myapp

This command will create a directory for your application at `apps/myapp` of your Fluo
install which will contain a `conf` and `lib`.

The `apps/myapp/conf` directory contains a copy of the `fluo.properties` from your default
configuration.  This should be configured for your application:

    vim apps/myapp/fluo.properties

When configuring the observer section in fluo.properties, you can configure your instance
for the [phrasecount] application if you have not created your own application. See
the [phrasecount] example for instructions. You can also choose not to configure any
observers but your workers will be idle when started.

The `apps/myapp/lib` directory should contain any observer jars for your application. If 
you configured [fluo.properties] for observers, copy any jars containing these
observer classes to this directory.
 
Initialize your application
---------------------------

After your application has been configured, use the command below to initialize it:

    fluo init myapp

This only needs to be called once and stores configuration in Zookeeper.

Start your application
----------------------

A Fluo application consists of one oracle process and multiple worker processes.
Before starting your application, you can configure the number of worker process
in your [fluo.properties] file.

When you are ready to start your Fluo application on your YARN cluster, run the
command below:

    fluo start myapp

The start command above will work for a single-node or a large cluster.  By
using YARN, you do not need to deploy the Fluo binaries to every node on your
cluster or start processes on every node.

You can use the following command to check the status of your instance:

    fluo status myapp

For more detailed information on the YARN containers running Fluo:

    fluo info myapp

You can also use `yarn application -list` to check the status of your Fluo instance
in YARN.  Logs are viewable within YARN.

When you have data in your fluo instance, you can view it using the command `fluo scan`.
Pipe the output to `less` using the command `fluo scan | less` if you want to page 
through the data.

Stop your Fluo application
--------------------------

Use the following command to stop your Fluo application:

    fluo stop myapp

If stop fails, there is also a kill command.

    fluo kill myapp

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

Tuning YARN
-----------

When running Fluo oracles and workers in YARN, the number of instances, max memory, and number
of cores for Fluo processes can be configured in [fluo.properties]. If YARN is killing processes
consider increasing `twill.java.reserved.memory.mb` (which defaults to 200 and is set in yarn-site.xml).
The `twill.java.reserved.memory.mb` config determines the gap between the YARN memory limit set in
[fluo.properties] and the java -Xmx setting.  For example, if max memory is 1024 and twill reserved
memory is 200, the java -Xmx setting will be 1024-200 = 824 MB.

Run locally without YARN
------------------------

If you do not have YARN set up, you can start the oracle and worker as a local 
Fluo process using the following commands:

    local-fluo start-oracle
    local-fluo start-worker

Use the following commands to stop a local Fluo process:

    local-fluo stop-worker
    local-fluo stop-oracle

In a distributed environment, you will need to deploy and configure a Fluo 
distribution on every node in your cluster.

[fluo-dev]: https://github.com/fluo-io/fluo-dev
[Accumulo]: https://accumulo.apache.org/
[Hadoop]: http://hadoop.apache.org/
[Zookeeper]: http://zookeeper.apache.org/
[Java]: https://www.oracle.com/java/index.html
[release]: https://github.com/apache/incubator-fluo/releases
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo.properties]: ../modules/distribution/src/main/config/fluo.properties
