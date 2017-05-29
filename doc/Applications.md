DXRAM provides a framework to easily develop small and big applications. Your application runs on one or multiple DXRAM peers independently but you can connect and synchronize them using services provided by the DXRAM core. This document gives you a brief introduction on how to get started with writing and deploying your own DXRAM application (abbreviated: *dxapp*).

# Examples

We provide examples that should get you started and also provide you with references to support with implementing your own dxapp.
The *HelloWorldApplication* (*dxhelloworld* sub-package) shows a simple hello world example. *HellWorldWithConfigApplicataion* adds configuration values that are available through a JSON configuration file which is automatically generated in the same folder as the jar file after the application is launched the first time.
Furthermore, the DXRAM terminal is also implemented as a dxapp with more advanced features.

# Setup ApplicationService, compilation and deployment

Here are the steps to setup, compile and deploy the *HelloWorldApplication*, manually. This application is actually compiled with the DXRAM project but we use it as an example to show you how to compile your own application here.

The *ApplicationService* must be enabled and running on the DXRAM peer(s) you want to run your dxapp(s) on. On the default configuration, it is enabled on all DXRAM peers but can be turned on/off in the DXRAM configuration file:
```
"ApplicationServiceConfig": {
  "m_class": "de.hhu.bsinfo.dxram.app.ApplicationServiceConfig",
  "m_serviceClass": "ApplicationService",
  "m_enabledForSuperpeer": false,
  "m_enabledForPeer": true
}
```
Ensure that *m_enabledForPeer* is set to *true*.

When the service is running, all jar-packages inside the *app* folder are scanned for classes implementing the *AbstractApplication* class. Instances of all found classes (multiple per jar-package possible) are created and the main-method is started in a separate thread.
Note: This happens after all core DXRAM services are initialized and started to ensure the node is fully booted before running user applications.

To compile and package the *HelloWorldApplication* example manually, run:
```
javac src/de/hhu/bsinfo/dxhelloworld/HelloWorldApplication.java -cp dxram.jar
jar cf dxhelloworld.jar src/de/hhu/bsinfo/dxhelloworld/HelloWorldApplication.class
```

You can also automate the build process by using build scripts (e.g. Ant). Checkout the build script *dxhelloworld-build.xml* which builds the *HelloWorldApplication* together with the DXRAM core. 

Move the output jar-package *dxhelloworld.jar* to the *app* folder and start your setup/cluster using the [deploy script](QuickStart.md). If you check the log files of the peer after you started the cluster, it should print the hello world message.

The hello world examples are straight forward and the documentation of the abstract methods of *AbstractApplication* provides the necessary information you need in order to implement your own dxapp.
