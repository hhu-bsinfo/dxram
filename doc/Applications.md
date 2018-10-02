DXRAM provides a framework to easily develop small and large applications. Your application runs on one or multiple
DXRAM peers independently but you can connect and synchronize them using [services](Services.md) provided by the DXRAM
core. This document gives you a brief introduction on how to get started with writing and deploying your own DXRAM
application (abbreviated: *dxapp*).

# Examples
We provide examples to get you started with implementing your own dxapp. The
[dxa-helloworld](https://github.com/hhu-bsinfo/dxapps/) application implements a simple hello world example. Further
applications like a command line interface to send commands to peer nodes
([dxa-dxterminal](https://github.com/hhu-bsinfo/dxapps/)) as well as several benchmark and test
applications used for developing DXRAM are also included in the same repository.

# Setup ApplicationService, compilation and deployment
These are the steps to setup, compile and deploy the *dxa-hellowrold* application. All dxapps must be compiled as a
separate jar file and linked against DXRAM (see the dedicated repository).

You have to compile your dxapp as a separate jar-package (most likely from a separate project). For starters, you
might want to pick one of the already implemented applications from the
[dxapps repository](https://github.com/hhu-bsinfo/dxapps/) and just write your code in *dxa-helloworld*. Later, you can
take care of a clean build system setup if you need to. The hello-world example is minimalistic but gives you a
basic framework to start writing your own applications.

All jar-packages must be placed inside a configurable folder (default: *dxapp*). These are scanned by DXRAM for classes
implementing the *AbstractApplication* class. All sub-classes found (multiple per jar-package possible) are
bootstrapped by the ApplicationService on startup.

There two methods to make DXRAM run your application: Add it to the autostart list in the configuration file or
run it using the DXRAM API.

Below is an excerpt of a configuration file that puts the dxa-helloworld application into autostart to run it once
the peer has finished initializing:
```
"ApplicationServiceConfig": {
"m_autoStart": [
    {
        "m_className": "de.hhu.bsinfo.dxapp.HelloWorld",
        "m_args": "123",
        "m_startOrderId": 0
    }
],
"m_classConfig": "de.hhu.bsinfo.dxram.app.ApplicationServiceConfig"
},
```

Make sure that your class name matches the fully qualified class path in your compiled jar. You can also pass command
line arguments to your application using the configuration file. The *m_startOrderId* determines the start order
when multiple applications are listed.

The second option to run applications is the DXRAM API. You can access the *ApplicationService* and run applications
that are available in the dxapp folder. This is used by our command line application
[dxa-dxterminal](https://github.com/hhu-bsinfo/dxapps/). Follow the instructions inside the repository on how to
setup the terminal and check the list of available commands. This allows you to run applications dynamically and
also supports running them repeatedly.
