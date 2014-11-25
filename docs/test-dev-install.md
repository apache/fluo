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

Test and Development Installation
=================================

Below are instructions for running Fluo in a test/development environment.
These instructions start a local Fluo instance called MiniFluo that runs
its own Accumulo and Zookeeper.  While MiniFluo is easy to set up and has 
all of the features of Fluo, it is not recommended for production use as 
all of its data is lost when its stopped.
   
If you looking to run Fluo in a production environment, check out the
[production installation instructions](production-install.md).  

Obtaining a distribution
------------------------

Before you can install Fluo, you will need to obtain a distribution tarball.  You
can either download the [latest release][release] or build a distribution tarball
by following these steps:

First, clone Fluo:
```
git clone https://github.com/fluo-io/fluo.git
cd fluo/
```
Optionally, check out a stable tag (if you don't want to build a release from master):
```
git checkout 1.0.0-beta-1
```
Next, build a distribution which will be created in `modules/distribution/target`.
```
mvn package
```

Installing and configuring MiniFluo
-----------------------------------

When you have a distribution tarball, follow these steps to install and configure Fluo.

First, choose a directory with plenty of space and untar the distribution:
```
tar -xvzf fluo-1.0.0-beta-1-SNAPSHOT-bin.tar.gz
```
Next, copy the example configuration to the base of your configuration directory:
```
cp conf/examples/* conf/
```

Next, view and optionally configure [fluo.properties]. 
```
vim conf/fluo.properties
```
As you are running a MiniFluo instance, most properties in [fluo.properties] do not need to be 
set by you except for the "Observer properties". For example, all "Client properties" can be left 
unset as MiniFluo will start up its own Accumulo and Zookeeper.  If you would rather have MiniFluo
connect to an existing Accumulo instance, change `io.fluo.mini.start.accumulo` to `false` and 
set all "Client properties".  If you have an existing Accumulo instance, you should also consider
running a Fluo instance by following the [production installation instructions](production-install.md).

When configuring the "Observer" section in fluo.properties, you can configure your instance
for the [phrasecount] application if you have not created your own application. See
the [phrasecount] example for instructions. You can also choose not to configure any
observers but the workers of MiniFluo will be idle when started.

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

Running MiniFluo
----------------

A MiniFluo instance can be started with the following command:
```
fluo mini start
```
 
MiniFluo will output logs to the `logs/` directory of your Fluo installation.

MiniFluo starts its own cluster of Accumulo and Zookeeper.  All data for this
cluster is written by default to `mini/` directory of your Fluo installation 
but this can be configured in `fluo.properties`.  Logs for underlying cluster
can be viewed in `mini/logs`.

Due to MiniFluo starting its own cluster, it generates and writes a `client.properties`
file to its data directory.  This file can be used by Fluo clients to connect
to the MiniFluo instance.

When you have data in your fluo instance, you can view it using the command `fluo scan`.
Pipe the output to `less` using the command `fluo scan | less` if you want to page 
through the data.

Stopping MiniFluo
-----------------

MiniFluo can be stopped by running the following command:
```
fluo mini stop
```
If MiniFluo started its own Accumulo cluster, it will also remove the `mini/` data 
directory and any data in your instance will be lost.

[release]: https://github.com/fluo-io/fluo/releases
[phrasecount]: https://github.com/fluo-io/phrasecount
[fluo.properties]: ../modules/distribution/src/main/config/fluo.properties
