package de.uniduesseldorf.dxram.core.api;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.commands.CommandHandler;
import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core.ShutdownThread;
import de.uniduesseldorf.dxram.core.api.nodeconfig.NodeID;
import de.uniduesseldorf.dxram.core.chunk.ChunkInterface;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfigurationHelper;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.recovery.RecoveryInterface;

import de.uniduesseldorf.menet.NetworkInterface;
import de.uniduesseldorf.utils.StatisticsManager;
import de.uniduesseldorf.utils.config.Configuration;
import de.uniduesseldorf.utils.config.ConfigurationHelper;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.utils.locks.JNILock;

public final class DXRAM 
{
	// Constants
	private final Logger LOGGER = Logger.getLogger(Core.class);
	
	// Attributes
	private ConfigurationHelper m_configurationHelper;
	private NodesConfigurationHelper m_nodesConfigurationHelper;
	
	public DXRAM()
	{
		
	}
	
	// Methods
	/**
	 * Initializes DXRAM<br>
	 * Should be called before any other method call of DXRAM
	 * @param p_configuration
	 *            the configuration to use
	 * @param p_nodesConfiguration
	 *            the nodes configuration to use
	 */
	public void initialize(final Configuration p_configuration, final NodesConfiguration p_nodesConfiguration) {
		int interval;

		LOGGER.trace("Entering initialize with: p_configuration=" + p_configuration + ", p_nodesConfiguration=" + p_nodesConfiguration);

		try {
			p_configuration.makeImmutable();
			m_configurationHelper = new ConfigurationHelper(p_configuration);

			m_nodesConfigurationHelper = new NodesConfigurationHelper(p_nodesConfiguration);

			JNILock.load(m_configurationHelper.getStringValue(DXRAMConfigurationConstants.JNI_LOCK_DIRECTORY));
			
			CoreComponentFactory.getNetworkInterface();
			m_chunk = CoreComponentFactory.getChunkInterface();

			m_network = CoreComponentFactory.getNetworkInterface();

			if (Core.getConfiguration().getBooleanValue(DXRAMConfigurationConstants.LOG_ACTIVE) && NodeID.getRole().equals(Role.PEER)) {
				CoreComponentFactory.getLogInterface();
			}
			m_recovery = CoreComponentFactory.getRecoveryInterface();

			registerCmdListener(new CommandHandler());

			// Register shutdown thread
			Runtime.getRuntime().addShutdownHook(new ShutdownThread());

			interval = Core.getConfiguration().getIntValue(DXRAMConfigurationConstants.STATISTIC_PRINT);
			if (interval > 0) {
				if (!NodeID.getRole().equals(Role.MONITOR)) {
				StatisticsManager.setupOutput(interval);
				}
			}
		} catch (final Exception e) {
			LOGGER.fatal("FATAL::Could not instantiate DXRAM", e);

			handleException(e, ExceptionSource.DXRAM_INITIALIZE);
		}

		LOGGER.trace("Exiting initialize");
	}

	/**
	 * Closes DXRAM and frees unused resources
	 */
	public void close() {
		LOGGER.trace("Entering close");

		CoreComponentFactory.closeAll();

		LOGGER.trace("Exiting close");
	}
}
