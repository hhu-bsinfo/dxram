
package de.uniduesseldorf.dxram.core;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.chunk.ChunkInterface;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.ComponentCreationException;
import de.uniduesseldorf.dxram.core.lock.LockInterface;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.recovery.RecoveryInterface;

import de.uniduesseldorf.menet.NetworkInterface;
import de.uniduesseldorf.utils.ZooKeeperHandler;
import de.uniduesseldorf.utils.ZooKeeperHandler.ZooKeeperException;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.utils.config.Configuration.ConfigurationEntry;

/**
 * Creates the components for DXRAM
 * @author Florian Klein
 *         09.03.2012
 */
public final class CoreComponentFactory {

	// Attributes
	private static ChunkInterface m_chunk;
	private static LogInterface m_log;
	private static LookupInterface m_lookup;
	private static NetworkInterface m_network;
	private static RecoveryInterface m_recovery;
	private static LockInterface m_lock;

	// Constructors
	/**
	 * Creates an instance of CoreComponentFactory
	 */
	private CoreComponentFactory() {}

	// Methods
	/**
	 * Returns the current ChunkInterface-Instance. If no instance exist, a new will be created.
	 * @return the current ChunkInterface-Instance
	 * @throws DXRAMException
	 *             if the ChunkInterface-Instance could not be initialized
	 */
	public static synchronized ChunkInterface getChunkInterface() throws DXRAMException {
		if (m_chunk == null) {
			m_chunk = newInstance(DXRAMConfigurationConstants.INTERFACE_CHUNK, ChunkInterface.class);
			m_chunk.initialize();
		}

		return m_chunk;
	}

	/**
	 * Closes the current ChunkInterface-Instance
	 */
	public static synchronized void closeChunkInterface() {
		if (m_chunk != null) {
			m_chunk.close();
			m_chunk = null;
		}
	}

	/**
	 * Returns the current LogInterface-Instance. If no instance exist, a new will be created.
	 * @return the current LogInterface-Instance
	 * @throws DXRAMException
	 *             if the LogInterface-Instance could not be initialized
	 */
	public static synchronized LogInterface getLogInterface() throws DXRAMException {
		if (m_log == null) {
			m_log = newInstance(DXRAMConfigurationConstants.INTERFACE_LOG, LogInterface.class);
			m_log.initialize();
		}

		return m_log;
	}

	/**
	 * Closes the current LogInterface-Instance
	 */
	public static synchronized void closeLogInterface() {
		if (m_log != null) {
			m_log.close();
			m_log = null;
		}
	}

	/**
	 * Returns the current LookupInterface-Instance. If no instance exist, a new will be created.
	 * @return the current LookupInterface-Instance
	 * @throws DXRAMException
	 *             if the LookupInterface-Instance could not be initialized
	 */
	public static synchronized LookupInterface getLookupInterface() throws DXRAMException {
		if (m_lookup == null) {
			m_lookup = newInstance(DXRAMConfigurationConstants.INTERFACE_LOOKUP, LookupInterface.class);
			m_lookup.initialize();
		}

		return m_lookup;
	}

	/**
	 * Closes the current LookupInterface-Instance
	 */
	public static synchronized void closeLookupInterface() {
		if (m_lookup != null) {
			m_lookup.close();
			m_lookup = null;
		}
	}

	/**
	 * Returns the current NetworkInterface-Instance. If no instance exist, a new will be created.
	 * @return the current NetworkInterface-Instance
	 * @throws DXRAMException
	 *             if the NetworkInterface-Instance could not be initialized
	 */
	public static synchronized NetworkInterface getNetworkInterface() throws DXRAMException {
		if (m_network == null) {
			m_network = newInstance(DXRAMConfigurationConstants.INTERFACE_NETWORK, NetworkInterface.class);
			m_network.initialize();
		}

		return m_network;
	}

	/**
	 * Closes the current NetworkInterface-Instance
	 */
	public static synchronized void closeNetworkInterface() {
		if (m_network != null) {
			m_network.close();
			m_network = null;
		}
	}

	/**
	 * Returns the current LockInterface-Instance. If no instance exist, a new will be created.
	 * @return the current LockInterface-Instance
	 * @throws DXRAMException
	 *             if the LockInterface-Instance could not be initialized
	 */
	public static synchronized LockInterface getLockInterface() throws DXRAMException {
		if (m_lock == null) {
			m_lock = newInstance(DXRAMConfigurationConstants.INTERFACE_LOCK, LockInterface.class);
			m_lock.initialize();
		}

		return m_lock;
	}

	/**
	 * Closes the current LockInterface-Instance
	 */
	public static synchronized void closeLockInterface() {
		if (m_lock != null) {
			m_lock.close();
			m_lock = null;
		}
	}

	/**
	 * Returns the current RecoveryInterface-Instance. If no instance exist, a new will be created.
	 * @return the current RecoveryInterface-Instance
	 * @throws DXRAMException
	 *             if the RecoveryInterface-Instance could not be initialized
	 */
	public static synchronized RecoveryInterface getRecoveryInterface() throws DXRAMException {
		if (m_recovery == null) {
			m_recovery = newInstance(DXRAMConfigurationConstants.INTERFACE_RECOVERY, RecoveryInterface.class);
			m_recovery.initialize();
		}

		return m_recovery;
	}

	/**
	 * Closes the current RecoveryInterface-Instance
	 */
	public static synchronized void closeRecoveryInterface() {
		if (m_recovery != null) {
			m_recovery.close();
			m_recovery = null;
		}
	}

	/**
	 * p_next
	 * Closes all instances
	 */
	public static synchronized void closeAll() {
		LookupInterface lookup;

		lookup = m_lookup;

		// Stop creating new connections
		if (m_network != null) {
			m_network.deactivateConnectionManager();
		}

		closeChunkInterface();
		closeLogInterface();
		closeLookupInterface();

		try {
			if (lookup != null) {
				ZooKeeperHandler.close(lookup.isLastSuperpeer());
			}
		} catch (final ZooKeeperException e) {
			// TODO: auf Fehler reagieren
			e.printStackTrace();
		}

		closeNetworkInterface();
		closeLockInterface();
		closeRecoveryInterface();
	}

	/**
	 * Creates an instance of the given CoreComponent class
	 * @param p_entry
	 *            the ConfigurationEntry for the CoreComponent
	 * @param p_class
	 *            the CoreComponent class
	 * @param <T>
	 *            the CoreComponent type
	 * @return the CoreComponent
	 * @throws DXRAMException
	 *             if the CoreComponent could not be created
	 */
	private static <T extends CoreComponent> T newInstance(final ConfigurationEntry<String> p_entry, final Class<T> p_class) throws DXRAMException {
		T ret = null;
		Object o;

		try {
			o = Class.forName(Core.getConfiguration().getStringValue(p_entry)).newInstance();
			if (p_class.isAssignableFrom(o.getClass())) {
				ret = p_class.cast(o);
			}
		} catch (final Exception e) {
			throw new ComponentCreationException("Could not create component", e);
		}

		return ret;
	}

}
