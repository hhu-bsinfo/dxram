# IDE configuration
We are using IntelliJ for development and provide setup guidance for it. Any other IDE supporting Java (like Eclipse)
should work as well but requires you to take care of the setup on your own.

## Style Guidelines
We try to keep a consistent style on the code base by using IntelliJ's built in formatter and inspection options. The
desired settings are exported and stored in the *intellij* subdirectory. There are a few things that cannot be applied
with IntelliJ's formatter options. Please read our [style guide](StyleGuideJava.md) and follow them when contributing.

## Configuration with IntelliJ
Open IntelliJ and create a new project from existing sources and chose *gradle* under "project import from external
model".

For quick testing, you can create run configurations for a Superpeer and a Peer using the DXRAMMain class as the
application entry point and the following VM arguments for the Superpeer:
```
-Dlog4j.configurationFile=config/log4j.xml
-Ddxram.config=config/dxram.json
-Ddxram.m_config.m_engineConfig.m_address.m_ip=127.0.0.1
-Ddxram.m_config.m_engineConfig.m_address.m_port=22221
-Ddxram.m_config.m_engineConfig.m_role=Superpeer
```
...and the Peer:
```
-Dlog4j.configurationFile=config/log4j.xml
-Ddxram.config=config/dxram.json
-Ddxram.m_config.m_engineConfig.m_address.m_ip=127.0.0.1
-Ddxram.m_config.m_engineConfig.m_address.m_port=22222
-Ddxram.m_config.m_engineConfig.m_role=Peer
-Ddxram.m_config.m_componentConfigs[MemoryManagerComponentConfig].m_keyValueStoreSize.m_value=128
-Ddxram.m_config.m_componentConfigs[MemoryManagerComponentConfig].m_keyValueStoreSize.m_unit=mb
```

Or use our deployment system [cdepl](https://github.com/hhu-bsinfo/cdepl)

## Building
Either run our build system using the *build.sh* file in the root directory or build from within the IDE using its
built in system.

## Debugging
Depending on the situation, you have different options to debug DXRAM and/or the application running on it:
* Use the debugger of your IDE to either start a DXRAM process with the debugger attached or attach remotely to an
already running DXRAM process.
* Our [terminal](https://github.com/hhu-bsinfo/dxapps) allows sending built in commands to trigger certain actions
or read various status information of the system.
