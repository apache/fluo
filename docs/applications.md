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

Running Fluo applications
=========================

Once you have Fluo installed and running on your cluster, you can now run Fluo applications which consist of 
clients and observers.

If you are new to Fluo, consider first running the [phrasecount] application on your Fluo instance.  Otherwise,
you can create your own Fluo client or observer by the following the steps below.
 
For both clients and observers, you will need to include the following in your Maven pom:
```xml
<dependency>
  <groupId>io.fluo</groupId>
  <artifactId>fluo-api</artifactId>
  <version>1.0.0-beta-1</version>
</dependency>
<dependency>
  <groupId>io.fluo</groupId>
  <artifactId>fluo-core</artifactId>
  <version>1.0.0-beta-1</version>
  <scope>runtime</scope>
</dependency>
```

Creating a Fluo client
----------------------

To create a [FluoClient], you will need to provide it with a [FluoConfiguration] object that is configured
to connect to your Fluo instance.  

If you have access to the [fluo.properties] file that was used to configure your Fluo instance, you can use
it to build a [FluoConfiguration] object with all necessary properties which are all properties with the 
`io.fluo.client.*` prefix in [fluo.properties]:
```java
FluoConfiguration config = new FluoConfiguration(new File("fluo.properties"));
```

You can also create an empty [FluoConfiguration] object and set properties using Java:
```java
FluoConfiguration config = new FluoConfiguration();
config.setAccumuloUser("user");
config.setAccumuloPassword("pass");
config.setAccumuloInstance("instance");
```

Once you have [FluoConfiguration] object, pass it to the `newClient()` method of [FluoFactory] to create a [FluoClient]:
```java
FluoClient client = FluoFactory.newClient(config)
```

Creating a Fluo observer
------------------------

To create an observer, follow these steps:

1. Create a class that extends [AbstractObserver].
2. Build a jar containing this class and include this jar in ```lib/observers``` of your Fluo installation.
3. Configure your Fluo instance to use this observer by modifying the Observer section of [fluo.properties].  
4. Restart your Fluo instance so that your Fluo workers load the new observer.

[phrasecount]: https://github.com/fluo-io/phrasecount
[FluoFactory]: modules/api/src/main/java/io/fluo/api/client/FluoFactory.java
[FluoClient]: modules/api/src/main/java/io/fluo/api/client/FluoClient.java
[FluoConfiguration]: modules/api/src/main/java/io/fluo/api/config/FluoConfiguration.java
[AbstractObserver]: modules/api/src/main/java/io/fluo/api/observer/AbstractObserver.java
[fluo.properties]: modules/distribution/src/main/config/fluo.properties
