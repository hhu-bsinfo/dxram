## IDE configuration
We are using IntelliJ for development and provide setup guidance for it.
Any other IDE supporting Java (like Eclipse) should work as well but
requires you to take care of the setup on your own.

### Configuration with IntelliJ
Open IntelliJ and create a new project from existing sources and 
choose the dxram folder from your workspace. Add the libraries under
_Project Structure_ in the _Libraries_ tab. Create run configurations
for a Superpeer and a Peer using the DXRAMMain class as the application
entry point and the following VM arguments for the Superpeer:
```
-Dlog4j.configurationFile=config/log4j.xml
-Ddxram.config=config/dxram.json
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22221
-Ddxram.m_engineSettings.m_role=Superpeer
```
...and the Peer:
```
-Dlog4j.configurationFile=config/log4j.xml
-Ddxram.config=config/dxram.json
-Ddxram.m_engineSettings.m_address.m_ip=127.0.0.1
-Ddxram.m_engineSettings.m_address.m_port=22222
-Ddxram.m_engineSettings.m_role=Peer
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_value=128
-Ddxram.m_components[MemoryManagerComponent].m_keyValueStoreSize.m_unit=mb
```
