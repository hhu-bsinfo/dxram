DXRAM provides a framework to easily develop small and big applications. Your application runs on one or multiple
DXRAM peers independently but you can connect and synchronize them using services provided by the DXRAM core. This
document gives you a brief introduction on how to get started with writing and deploying your own DXRAM application
(abbreviated: *dxapp*).

# Examples
We provide examples to get you started with implementing your own dxapp. The
[HelloWorldApplication](https://github.com/hhu-bsinfo/dxapp-helloworld/) implements a simple hello world example.
*HellWorldWithConfigApplicataion* adds configuration values that are available through a JSON configuration file which
is automatically generated in the same folder as the jar file after the application is launched the first time.
For further reference, [DXTerm](https://github.com/hhu-bsinfo/dxterm/), DXRAM's terminal, is also implemented as a
dxapp with more advanced features.

# Setup ApplicationService, compilation and deployment
Here are the steps to setup, compile and deploy the *HelloWorldApplication*. The application is compiled as a separate
project and linked against DXRAM (see the dedicated repository).

The *ApplicationService* must be enabled and running on the DXRAM peer(s) you want to run your dxapp(s) on. On the
default configuration, it is enabled on all DXRAM peers but can be turned on/off in the DXRAM configuration file:
```
"ApplicationServiceConfig": {
  "m_class": "de.hhu.bsinfo.dxram.app.ApplicationServiceConfig",
  "m_serviceClass": "ApplicationService",
  "m_enabledForSuperpeer": false,
  "m_enabledForPeer": true
}
```
Ensure that *m_enabledForPeer* is set to *true*.

When the service is running, all jar-packages inside the *dxapp* folder are scanned for classes implementing the
*AbstractApplication* class. Instances of all found classes (multiple per jar-package possible) are created and the
main-method is started in a separate thread.

Note: This happens after all core DXRAM services are initialized and started to ensure the node is fully booted before
running user applications.

The hello world examples are straight forward and the documentation of the abstract methods of
*AbstractApplication* provides the necessary information you need in order to implement your own dxapps.

# DXApp using further dependencies (external jars)
If you need further dependencies for your applications loaded to the JVM before your dxapp is loaded,
because your dxapp uses them, create an implementation of *AbstractApplicationDependency* and put it
into the same jar file as your dxapp. The application loader of DXRAM scans the jar for all classes
implementating *AbstractApplicationDependency* and loads the dependencies returned by the *getDependencies*
method.

# DXApps using other DXApps
You can also compile a dxapp package that is used by another dxapp as some sort of library. Use the main method for
library initialization and don't run any "processing code". If main returns, the dxapp is not unloaded. However, you
have to ensure that your "library" is loaded before your dxapp using it. Enable your dxapp to use a configuration file
(return true on *useConfigurationFile*, see examples) and configure the *m_startOrderId* accordingly.
