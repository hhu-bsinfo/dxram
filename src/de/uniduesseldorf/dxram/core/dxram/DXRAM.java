package de.uniduesseldorf.dxram.core.dxram;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.DXRAMEngineSetupHandler;
import de.uniduesseldorf.dxram.core.backup.BackupComponent;
import de.uniduesseldorf.dxram.core.chunk.ChunkService;
import de.uniduesseldorf.dxram.core.engine.DXRAMEngine;
import de.uniduesseldorf.dxram.core.engine.DXRAMService;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationException;
import de.uniduesseldorf.dxram.core.lock.DefaultLockComponent;
import de.uniduesseldorf.dxram.core.log.LogComponent;
import de.uniduesseldorf.dxram.core.lookup.DefaultLookupComponent;
import de.uniduesseldorf.dxram.core.mem.MemoryManagerComponent;
import de.uniduesseldorf.dxram.core.net.NetworkComponent;

import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.config.ConfigurationException;

public final class DXRAM implements DXRAMEngineSetupHandler
{
	private final Logger LOGGER = Logger.getLogger(DXRAM.class);
	
	private DXRAMEngine m_engine;
	
	public DXRAM()
	{
		m_engine = new DXRAMEngine(this);
	}
	
	public boolean initialize(final String p_configurationFolder) {
		return initialize(p_configurationFolder, null, null, null);
	}
	
	public boolean initialize(final String p_configurationFolder, final String p_overrideIp, 
			final String p_overridePort, final String p_overrideRole) {
		boolean success = false;
		
		try {
			success = m_engine.init(p_configurationFolder);
		} catch (ConfigurationException e) {
			System.out.println("Initializing DXRAM failed, configuration exception: " + e.getMessage());
		} catch (NodesConfigurationException e) {
			System.out.println("Initializing DXRAM failed, nodes configuration exception: " + e.getMessage());
		}
		
		return success;
	}
	
	@SuppressWarnings("unchecked")
	protected <T extends DXRAMService> T getService(final String p_serviceName)
	{		   
		return (T) m_engine.getService(p_serviceName);
	}

	/**
	 * Closes DXRAM and frees unused resources
	 */
	public void shutdown() {
		LOGGER.info("Shutting down DXRAM...");

		m_engine.shutdown();

		LOGGER.info("Shutting down DXRAM done.");
	}

	@Override
	public void setupComponents(final DXRAMEngine p_engine, final Configuration p_configuration) 
	{
		// TODO read from configuration and setup
		p_engine.addComponent(new MemoryManagerComponent(0, 5));
		p_engine.addComponent(new NetworkComponent(1, 4));
		p_engine.addComponent(new DefaultLockComponent(2, 3));
		p_engine.addComponent(new DefaultLookupComponent(3, 2));
		p_engine.addComponent(new LogComponent(4, 1));
		p_engine.addComponent(new BackupComponent(5, 0));
	}

	@Override
	public void setupServices(final DXRAMEngine p_engine, final Configuration p_configuration) {
		// TODO read from configuration and setup
		//p_engine.addService(new ChunkService());
	}
}
