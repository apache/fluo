Fluo Dependencies
-----------------

This directory provides a simple maven pom that can be used to fetch Accumulo,
Hadoop, and Zookeeper depedencies.   You should fetch the versions of these
dependencies that are installed on your system.

    cd $FLUO_HOME/lib/deps/
    #edit pom to reflect versions of Software on your system
    vim pom.xml
    rm *.jar;mvn package clean

After doing this edit `$FLUO_HOME/conf/fluo-env.sh` to add
`$FLUO_HOME/lib/deps/*` to the classpath.
