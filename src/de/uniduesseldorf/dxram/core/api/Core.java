
package de.uniduesseldorf.dxram.core.api;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.config.Configuration;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.ConfigurationHelper;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.api.config.NodesConfigurationHelper;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.ChunkInterface;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.CommandMessage;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.CommandRequest;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages.CommandResponse;
import de.uniduesseldorf.dxram.core.events.IncomingChunkListener;
import de.uniduesseldorf.dxram.core.exceptions.ChunkException;
import de.uniduesseldorf.dxram.core.exceptions.ComponentCreationException;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMRuntimeException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.exceptions.PrimaryLogException;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.NameServiceStringConverter;
import de.uniduesseldorf.dxram.utils.StatisticsManager;

/**
 * API for DXRAM
 * @author Florian Klein 09.03.2012
 */
public final class Core {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(Core.class);

	// Attributes
	private static ConfigurationHelper m_configurationHelper;
	private static NodesConfigurationHelper m_nodesConfigurationHelper;

	private static NetworkInterface m_network;
	private static ChunkInterface m_chunk;
	private static ExceptionHandler m_exceptionHandler;

	// Constructors
	/**
	 * Creates an instance of DXRAM
	 */
	private Core() {}

	// Getters
	/**
	 * Get the DXRAM configuration
	 * @return the configuration
	 */
	public static ConfigurationHelper getConfiguration() {
		return m_configurationHelper;
	}

	/**
	 * Get the parsed DXRAM nodes configuration
	 * @return the parsed nodes configuration
	 */
	public static NodesConfigurationHelper getNodesConfiguration() {
		return m_nodesConfigurationHelper;
	}

	// Setters
	/**
	 * Set the IncomingChunkListener for DXRAM
	 * @param p_listener
	 *            the IncomingChunkListener
	 */
	public static void setListener(final IncomingChunkListener p_listener) {
		m_chunk.setListener(p_listener);
	}

	/**
	 * Set the ExceptionHandler for DXRAM
	 * @param p_exceptionHandler
	 *            the ExceptionHandler
	 */
	public static void setExceptionHandler(final ExceptionHandler p_exceptionHandler) {
		m_exceptionHandler = p_exceptionHandler;
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
	public static void initialize(final Configuration p_configuration, final NodesConfiguration p_nodesConfiguration) {
		LOGGER.trace("Entering initialize with: p_configuration=" + p_configuration + ", p_nodesConfiguration="
				+ p_nodesConfiguration);

		try {
			p_configuration.makeImmutable();
			m_configurationHelper = new ConfigurationHelper(p_configuration);

			m_nodesConfigurationHelper = new NodesConfigurationHelper(p_nodesConfiguration);

			CoreComponentFactory.getNetworkInterface();
			m_chunk = CoreComponentFactory.getChunkInterface();

			m_network = CoreComponentFactory.getNetworkInterface();

			if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.LOG_ACTIVE)
					&& NodeID.getRole().equals(Role.PEER)) {
				CoreComponentFactory.getLogInterface();
			}

			// Register shutdown thread
			Runtime.getRuntime().addShutdownHook(new ShutdownThread());

			StatisticsManager.setupOutput(60);
		} catch (final Exception e) {
			LOGGER.fatal("FATAL::Could not instantiate DXRAM", e);

			handleException(e, ExceptionSource.DXRAM_INITIALIZE);
		}

		LOGGER.trace("Exiting initialize");
	}

	/**
	 * Closes DXRAM and frees unused resources
	 */
	public static void close() {
		LOGGER.trace("Entering close");

		CoreComponentFactory.closeAll();

		LOGGER.trace("Exiting close");
	}

	/**
	 * Get the own NodeID
	 * @return the own NodeID
	 */
	public static short getNodeID() {
		return NodeID.getLocalNodeID();
	}

	/**
	 * Creates a new Chunk
	 * @param p_size
	 *            the size of the Chunk
	 * @return a new Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be created
	 */
	public static Chunk createNewChunk(final int p_size) throws DXRAMException {
		Chunk ret = null;

		try {
			if (m_chunk != null) {
				ret = m_chunk.create(p_size);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_CREATE_NEW_CHUNK);
		}

		return ret;
	}

	/**
	 * Creates new Chunks
	 * @param p_sizes
	 *            the sizes of the Chunks
	 * @return new Chunks
	 * @throws DXRAMException
	 *             if the chunks could not be created
	 */
	public static Chunk[] createNewChunk(final int[] p_sizes) throws DXRAMException {
		Chunk[] ret = null;

		try {
			if (m_chunk != null) {
				ret = m_chunk.create(p_sizes);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_CREATE_NEW_CHUNK);
		}

		return ret;
	}

	/**
	 * Creates a new Chunk with identifier
	 * @param p_size
	 *            the size of the Chunk
	 * @param p_name
	 *            the identifier of the Chunk
	 * @return a new Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be created
	 */
	public static Chunk createNewChunk(final int p_size, final String p_name) throws DXRAMException {
		Chunk ret = null;

		try {
			if (m_chunk != null) {
				ret = m_chunk.create(p_size, NameServiceStringConverter.convert(p_name));
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_CREATE_NEW_CHUNK);
		}

		return ret;
	}

	/**
	 * Creates new Chunks with identifier
	 * @param p_sizes
	 *            the sizes of the Chunks
	 * @param p_name
	 *            the identifier of the first Chunk
	 * @return new Chunks
	 * @throws DXRAMException
	 *             if the chunks could not be created
	 */
	public static Chunk[] createNewChunk(final int[] p_sizes, final String p_name) throws DXRAMException {
		Chunk[] ret = null;

		try {
			if (m_chunk != null) {
				ret = m_chunk.create(p_sizes, NameServiceStringConverter.convert(p_name));
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_CREATE_NEW_CHUNK);
		}

		return ret;
	}

	/**
	 * Gets the corresponding Chunk for the given ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public static Chunk get(final long p_chunkID) throws DXRAMException {
		Chunk ret = null;

		ChunkID.check(p_chunkID);

		try {
			if (m_chunk != null) {
				ret = m_chunk.get(p_chunkID);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_GET, p_chunkID);
		}

		return ret;
	}

	/**
	 * Gets the corresponding Chunks for the given IDs
	 * @param p_chunkIDs
	 *            the IDs of the corresponding Chunks
	 * @return the Chunks for the given IDs
	 * @throws DXRAMException
	 *             if the chunks could not be get
	 */
	public static Chunk[] get(final long[] p_chunkIDs) throws DXRAMException {
		Chunk[] ret = null;

		Contract.checkNotNull(p_chunkIDs, "no IDs given");
		ChunkID.check(p_chunkIDs);

		try {
			if (m_chunk != null) {
				ret = m_chunk.get(p_chunkIDs);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_GET, p_chunkIDs);
		}

		return ret;
	}

	/**
	 * Gets the corresponding Chunk for the given identifier
	 * @param p_name
	 *            the identifier of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public static Chunk get(final String p_name) throws DXRAMException {
		Chunk ret = null;
		int id;

		id = NameServiceStringConverter.convert(p_name);
		try {
			if (m_chunk != null) {
				ret = m_chunk.get(id);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_GET, id);
		}

		return ret;
	}

	/**
	 * Gets the corresponding ChunkID for the given identifier
	 * @param p_name
	 *            the identifier of the corresponding Chunk
	 * @return the ChunkID for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public static long getChunkID(final String p_name) throws DXRAMException {
		long ret = -1;
		int id;

		id = NameServiceStringConverter.convert(p_name);
		try {
			if (m_chunk != null) {
				ret = m_chunk.getChunkID(id);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_GET, id);
		}

		return ret;
	}

	/**
	 * Requests the corresponding Chunk for the given ID<br>
	 * An IncomingChunkEvent will be triggered on the arrival of the Chunk
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */
	public static void getAsync(final long p_chunkID) throws DXRAMException {
		ChunkID.check(p_chunkID);

		try {
			if (m_chunk != null) {
				m_chunk.getAsync(p_chunkID);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_GET_ASYNC, p_chunkID);
		}
	}

	/**
	 * Updates the given Chunk
	 * @param p_chunk
	 *            the Chunk to be updated
	 * @throws DXRAMException
	 *             if the chunk could not be put
	 */
	public static void put(final Chunk p_chunk) throws DXRAMException {
		put(p_chunk, false);
	}

	/**
	 * Updates the given Chunk
	 * @param p_chunk
	 *            the Chunk to be updated
	 * @param p_releaseLock
	 *            if true a possible lock is released
	 * @throws DXRAMException
	 *             if the chunk could not be put
	 */
	public static void put(final Chunk p_chunk, final boolean p_releaseLock) throws DXRAMException {
		Contract.checkNotNull(p_chunk, "no chunk given");

		try {
			if (m_chunk != null) {
				m_chunk.put(p_chunk, p_releaseLock);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_PUT, p_chunk);
		}
	}

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @throws DXRAMException
	 *             if the chunk could not be locked
	 */
	public static Chunk lock(final long p_chunkID) throws DXRAMException {
		return lock(p_chunkID, false);
	}

	/**
	 * Requests and locks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @return the Chunk for the given ID
	 * @param p_readLock
	 *            true if the lock is a read lock, false otherwise
	 * @throws DXRAMException
	 *             if the chunk could not be locked
	 */
	public static Chunk lock(final long p_chunkID, final boolean p_readLock) throws DXRAMException {
		Chunk ret = null;

		ChunkID.check(p_chunkID);

		try {
			if (m_chunk != null) {
				ret = m_chunk.lock(p_chunkID, p_readLock);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_LOCK, p_chunkID);
		}

		return ret;
	}

	/**
	 * Unlocks the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be unlocked
	 */
	public static void unlock(final long p_chunkID) throws DXRAMException {
		ChunkID.check(p_chunkID);

		try {
			if (m_chunk != null) {
				m_chunk.unlock(p_chunkID);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_LOCK, p_chunkID);
		}
	}

	/**
	 * Removes the corresponding Chunk for the giving ID
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be removed
	 */
	public static void remove(final long p_chunkID) throws DXRAMException {
		ChunkID.check(p_chunkID);

		try {
			if (m_chunk != null) {
				m_chunk.remove(p_chunkID);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_REMOVE, p_chunkID);
		}
	}

	/**
	 * Executes given command
	 * @param p_dest
	 *            NID of destination node for this request
	 * @param p_command
	 *            the command
	 * @param p_reply
	 *            true: want reply (will be handled as request)
	 * @throws DXRAMException
	 *             if the chunk could not be get
	 */

	public static String execute(final short p_dest, final String p_command, final boolean p_reply)
			throws DXRAMException {
		// request with reply
		if (p_reply) {
			System.out.println("Core.execute: p_dest=" + p_dest);
			CommandRequest request = new CommandRequest(p_dest, p_command);
			Contract.checkNotNull(request);
			try {
				request.sendSync(m_network);
			} catch (final NetworkException e) {
				System.out.println("error: sendSync failed in Core.execute:" + e.toString());
			}
			CommandResponse response = request.getResponse(CommandResponse.class);

			String result = response.getAnswer();
			return result;
		}
		// request without reply
		else {
			new CommandMessage(p_dest, p_command).send(m_network);
		}
		return null;
	}

	/*
	 * public static void execute(final String p_command, final String... p_args) throws DXRAMException {
	 * short type;
	 * 
	 * type = CommandStringConverter.convert(p_command);
	 * 
	 * switch (type) {
	 * case 1:
	 * // migrate: ChunkID, src, dest
	 * new CommandMessage(Short.parseShort(p_args[1]), type, p_args).send(m_network);
	 * break;
	 * case 2:
	 * // show_nodes:
	 * try {
	 * System.out.println("Superpeers:");
	 * System.out.println(ZooKeeperHandler.getChildren("nodes/superpeers").toString());
	 * System.out.println("Peers:");
	 * System.out.println(ZooKeeperHandler.getChildren("nodes/peers").toString());
	 * } catch (final ZooKeeperException e) {
	 * System.out.println("Could not access ZooKeeper!");
	 * }
	 * break;
	 * case -1:
	 * System.out.println("Command unknown!");
	 * break;
	 * default:
	 * break;
	 * }
	 * }
	 */

	/**
	 * Migrates the corresponding Chunk for the giving ID to another Node
	 * @param p_chunkID
	 *            the ID of the corresponding Chunk
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the chunk could not be migrated
	 */
	public static void migrate(final long p_chunkID, final short p_target) throws DXRAMException {
		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		try {
			if (m_chunk != null) {
				m_chunk.migrate(p_chunkID, p_target);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_MIGRATE, p_chunkID, p_target);
		}
	}

	/**
	 * Migrates the corresponding Chunks for the giving ID range to another Node
	 * @param p_startChunkID
	 *            the first ID
	 * @param p_endChunkID
	 *            the last ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the chunks could not be migrated
	 */
	public static void migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target)
			throws DXRAMException {

		ChunkID.check(p_startChunkID);
		ChunkID.check(p_endChunkID);
		NodeID.check(p_target);

		try {
			if (m_chunk != null) {
				m_chunk.migrateRange(p_startChunkID, p_endChunkID, p_target);
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_MIGRATE, p_startChunkID, p_target);
		}
	}

	/**
	 * Recovers the local data from the log
	 * @throws DXRAMException
	 *             if the chunks could not be recovered
	 */
	public static void recoverFromLog() throws DXRAMException {
		try {
			if (m_chunk != null) {
				m_chunk.recoverFromLog();
			}
		} catch (final DXRAMException e) {
			handleException(e, ExceptionSource.DXRAM_RECOVER_FROM_LOG);
		}
	}

	/**
	 * Handles an occured exception
	 * @param p_exception
	 *            the occured exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occured (optional)
	 */
	public static void handleException(final Exception p_exception, final ExceptionSource p_source,
			final Object... p_parameters) {
		boolean ret = false;

		LOGGER.error("ERROR in " + p_source.toString(), p_exception);

		if (m_exceptionHandler != null) {
			if (p_exception instanceof LookupException) {
				ret = m_exceptionHandler.handleException((LookupException) p_exception, p_source, p_parameters);
			} else if (p_exception instanceof ChunkException) {
				ret = m_exceptionHandler.handleException((ChunkException) p_exception, p_source, p_parameters);
			} else if (p_exception instanceof NetworkException) {
				ret = m_exceptionHandler.handleException((NetworkException) p_exception, p_source, p_parameters);
			} else if (p_exception instanceof PrimaryLogException) {
				ret = m_exceptionHandler.handleException((PrimaryLogException) p_exception, p_source, p_parameters);
			} else if (p_exception instanceof RecoveryException) {
				ret = m_exceptionHandler.handleException((RecoveryException) p_exception, p_source, p_parameters);
			} else if (p_exception instanceof ComponentCreationException) {
				ret =
						m_exceptionHandler.handleException((ComponentCreationException) p_exception, p_source,
								p_parameters);
			} else {
				ret = m_exceptionHandler.handleException(p_exception, p_source, p_parameters);
			}
		}

		if (!ret) {
			throw new DXRAMRuntimeException("Unhandled Exception", p_exception);
		}
	}

	// Classe
	/**
	 * Shuts down DXRAM in case of the system exits
	 * @author Florian Klein 03.09.2013
	 */
	private static final class ShutdownThread extends Thread {

		// Constructors
		/**
		 * Creates an instance of ShutdownThread
		 */
		private ShutdownThread() {
			super(ShutdownThread.class.getSimpleName());
		}

		// Methods
		@Override
		public void run() {
			close();
		}

	}

}
