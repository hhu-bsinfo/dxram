
package de.uniduesseldorf.dxram.core.log;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.log.LogMessages.InitRequest;
import de.uniduesseldorf.dxram.core.log.LogMessages.InitResponse;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogRequest;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.log.header.AbstractLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.DefaultPrimLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.DefaultPrimLogTombstone;
import de.uniduesseldorf.dxram.core.log.header.DefaultSecLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.LogEntryHeaderInterface;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogTombstone;
import de.uniduesseldorf.dxram.core.log.storage.LogCatalog;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryLog;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryWriteBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogWithSegments;
import de.uniduesseldorf.dxram.core.log.storage.VersionsHashTable;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.utils.Contract;

/**
 * Leads data accesses to a remote node
 * @author Kevin Beineke 29.05.2014
 */
public final class LogHandler implements LogInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	public static final int FLASHPAGE_SIZE = 4 * 1024;
	public static final int MAX_NODE_CNT = Short.MAX_VALUE * 2;
	public static final long PRIMLOG_SIZE = Core.getConfiguration().getLongValue(
			ConfigurationConstants.PRIMARY_LOG_SIZE);
	public static final int PRIMLOG_MIN_SIZE = MAX_NODE_CNT * FLASHPAGE_SIZE;
	public static final long SECLOG_SIZE = Core.getConfiguration().getLongValue(
			ConfigurationConstants.SECONDARY_LOG_SIZE);
	public static final int SECLOG_MIN_SIZE = 1024 * FLASHPAGE_SIZE;
	public static final int SECLOG_SEGMENT_SIZE = Core.getConfiguration()
			.getIntValue(ConfigurationConstants.LOG_SEGMENT_SIZE);
	public static final int SECLOG_DEFAULT_BUFFER_SIZE = FLASHPAGE_SIZE * 2;

	public static final int WRITE_BUFFER_SIZE = Core.getConfiguration().getIntValue(
			ConfigurationConstants.WRITE_BUFFER_SIZE);
	public static final int WRITE_BUFFER_MAX_SIZE = Integer.MAX_VALUE;
	public static final String BACKUP_DIRECTORY = Core.getConfiguration().getStringValue(
			ConfigurationConstants.LOG_DIRECTORY);
	public static final String PRIMLOG_FILENAME = "primary.log";
	public static final String SECLOG_PREFIX_FILENAME = "secondary";
	public static final String SECLOG_POSTFIX_FILENAME = ".log";
	public static final byte[] PRIMLOG_MAGIC = "DXRAMPrimLogv1".getBytes(Charset.forName("UTF-8"));
	public static final byte[] SECLOG_MAGIC = "DXRAMSecLogv1".getBytes(Charset.forName("UTF-8"));

	public static final double REORG_UTILIZATION_THRESHOLD = 70;
	// Must be smaller than 1/2 of WRITE_BUFFER_SIZE
	public static final int SIGNAL_ON_BYTE_COUNT = 64 * 1024 * 1024;
	public static final int MAX_BYTE_COUNT = 80 * 1024 * 1024;
	public static final short SECLOGS_REORG_SHUTDOWNTIME = 5;
	public static final long FLUSHING_WAITTIME = 1000L;
	public static final long WRITERTHREAD_TIMEOUTTIME = 500L;
	public static final long REORGTHREAD_TIMEOUT = 1000L;

	// Log header component sizes
	public static final byte LOG_ENTRY_TYP_SIZE = 1;
	public static final byte LOG_ENTRY_NID_SIZE = 2;
	public static final byte LOG_ENTRY_LID_SIZE = 6;
	public static final byte LOG_ENTRY_LEN_SIZE = 4;
	public static final byte LOG_ENTRY_VER_SIZE = 4;
	public static final byte LOG_ENTRY_RID_SIZE = 1;
	public static final byte LOG_ENTRY_SRC_SIZE = 2;
	public static final byte LOG_ENTRY_CRC_SIZE = 8;

	public static final int PRIMLOG_MAGIC_HEADER_SIZE = PRIMLOG_MAGIC.length;
	public static final int SECLOG_MAGIC_HEADER_SIZE = SECLOG_MAGIC.length;

	private static final LogEntryHeaderInterface DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
	private static final LogEntryHeaderInterface MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();
	private static final LogEntryHeaderInterface DEFAULT_PRIM_LOG_TOMBSTONE = new DefaultPrimLogTombstone();
	private static final LogEntryHeaderInterface MIGRATION_PRIM_LOG_TOMBSTONE = new MigrationPrimLogTombstone();

	private static final LogEntryHeaderInterface DEFAULT_SEC_LOG_ENTRY_HEADER = new DefaultSecLogEntryHeader();

	// Attributes
	private NetworkInterface m_network;

	private PrimaryWriteBuffer m_writeBuffer;
	private PrimaryLog m_primaryLog;
	private AtomicReferenceArray<LogCatalog> m_logCatalogs;

	private Lock m_secondaryLogCreationLock;

	private SecondaryLogsReorgThread m_secondaryLogsReorgThread;
	private Lock m_reorganizationLock;
	private Condition m_reorganizationFinishedCondition;
	private Condition m_thresholdReachedCondition;

	private AtomicBoolean m_flushingInProgress;

	private boolean m_isShuttingDown;

	private boolean m_reorgThreadWaits;
	private boolean m_accessGranted;

	// Constructors
	/**
	 * Creates an instance of LogHandler
	 */
	public LogHandler() {
		m_network = null;

		m_writeBuffer = null;
		m_primaryLog = null;
		m_logCatalogs = null;

		m_secondaryLogCreationLock = null;

		m_secondaryLogsReorgThread = null;
		m_reorganizationLock = null;
		m_reorganizationFinishedCondition = null;
		m_thresholdReachedCondition = null;

		m_flushingInProgress = null;
		m_isShuttingDown = false;
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {

		m_network = CoreComponentFactory.getNetworkInterface();
		m_network.register(LogRequest.class, this);
		m_network.register(LogMessage.class, this);
		m_network.register(RemoveMessage.class, this);
		m_network.register(InitRequest.class, this);

		// Create primary log
		try {
			m_primaryLog = new PrimaryLog(PRIMLOG_SIZE);
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error: Primary log creation failed");
		}

		// Create primary log buffer
		m_writeBuffer = new PrimaryWriteBuffer(m_primaryLog, WRITE_BUFFER_SIZE);

		// Create secondary log and secondary log buffer catalagues
		m_logCatalogs = new AtomicReferenceArray<LogCatalog>(LogHandler.MAX_NODE_CNT);

		m_secondaryLogCreationLock = new ReentrantLock();

		// Create reorganization thread for secondary logs
		m_secondaryLogsReorgThread = new SecondaryLogsReorgThread();
		// Start secondary logs reorganization thread
		m_secondaryLogsReorgThread.start();

		m_reorganizationLock = new ReentrantLock();
		m_reorganizationFinishedCondition = m_reorganizationLock.newCondition();
		m_thresholdReachedCondition = m_reorganizationLock.newCondition();

		m_flushingInProgress = new AtomicBoolean();
		m_flushingInProgress.set(false);
	}

	@Override
	public void close() {
		LogCatalog cat;

		m_isShuttingDown = true;

		// Stop reorganization thread
		m_reorganizationLock.lock();
		m_thresholdReachedCondition.signal();
		m_reorganizationLock.unlock();
		try {
			m_secondaryLogsReorgThread.join();
		} catch (final InterruptedException e1) {
			System.out.println("Could not close reorganization thread!");
		}
		m_secondaryLogsReorgThread = null;
		m_reorganizationFinishedCondition = null;
		m_thresholdReachedCondition = null;
		m_reorganizationLock = null;

		// Close write buffer
		try {
			m_writeBuffer.closeWriteBuffer();
		} catch (final IOException | InterruptedException e) {
			e.printStackTrace();
		}
		m_writeBuffer = null;

		// Close primary log
		if (m_primaryLog != null) {
			try {
				m_primaryLog.closeLog();
			} catch (final InterruptedException | IOException e) {
				System.out.println("Could not close primary log");
			}
			m_primaryLog = null;
		}

		// Clear secondary logs and buffers
		for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
			try {
				cat = m_logCatalogs.get(i);
				if (cat != null) {
					cat.closeLogsAndBuffers();
				}
			} catch (final IOException | InterruptedException e) {
				System.out.println("Could not close secondary log buffer " + i);
			}
		}
		m_logCatalogs = null;
	}

	@Override
	public void initBackupRange(final long p_firstChunkIDOrRangeID, final short[] p_backupPeers) {
		InitRequest request;
		InitResponse response;

		if (null != p_backupPeers) {
			for (int i = 0; i < p_backupPeers.length; i++) {
				if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
					request =
							new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID,
									ChunkID.getCreatorID(p_firstChunkIDOrRangeID));
				} else {
					request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID, NodeID.getLocalNodeID());
				}
				Contract.checkNotNull(request);
				try {
					request.sendSync(m_network);
				} catch (final NetworkException e) {
					i--;
					continue;
				}
				response = request.getResponse(InitResponse.class);

				if (!response.getStatus()) {
					i--;
				}
			}
		}

	}

	@Override
	public long logChunk(final Chunk p_chunk, final byte p_rangeID, final short p_source) throws DXRAMException {
		byte[] logHeader;

		// System.out.println("Logging Chunk: " + p_chunk.getChunkID() + ", " + p_chunk.getSize());

		if (p_rangeID == -1) {
			logHeader = DEFAULT_PRIM_LOG_ENTRY_HEADER.createHeader(p_chunk, (byte) -1, (short) -1);
		} else {
			logHeader = MIGRATION_PRIM_LOG_ENTRY_HEADER.createHeader(p_chunk, p_rangeID, p_source);
		}
		try {
			m_writeBuffer.putLogData(logHeader, p_chunk.getData().array());
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during logging (" + p_chunk.getChunkID() + ")!");
		}

		return 0;
	}

	@Override
	public void removeChunk(final long p_chunkID, final byte p_rangeID, final short p_source) throws DXRAMException {
		byte[] tombstone;

		if (p_rangeID == -1) {
			tombstone = DEFAULT_PRIM_LOG_TOMBSTONE.createHeader(null, (byte) -1, (short) -1);
			AbstractLogEntryHeader.putChunkID(tombstone, p_chunkID, DEFAULT_PRIM_LOG_TOMBSTONE.getNIDOffset(false));
		} else {
			tombstone = MIGRATION_PRIM_LOG_TOMBSTONE.createHeader(null, (byte) -1, (short) -1);
			AbstractLogEntryHeader.putChunkID(tombstone, p_chunkID, MIGRATION_PRIM_LOG_TOMBSTONE.getNIDOffset(false));
		}

		try {
			m_writeBuffer.putLogData(tombstone, null);
			getSecondaryLog(p_chunkID, p_source, p_rangeID).incDeleteCounter();
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during deletion (" + p_chunkID + ")!");
		}
	}

	@Override
	public void recoverAllLogEntries(final short p_nodeID, final long p_chunkID, final byte p_rangeID)
			throws DXRAMException {
		ArrayList<Chunk> chunkList = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			flushDataToPrimaryLog();

			secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_nodeID, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();
				chunkList = getSecondaryLog(p_chunkID, p_nodeID, p_rangeID).recoverAllLogEntries(true);
			}
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during recovery");
		}

		if (chunkList != null) {
			// TODO: Handle recovered chunks
		}

	}

	@Override
	public void recoverRange(final short p_nodeID, final long p_low, final long p_high) throws DXRAMException {
		// ArrayList<Chunk> chunkList = null;
		// SecondaryLogBuffer secondaryLogBuffer;

		try {
			flushDataToPrimaryLog();
			// TODO:
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during recovery");
		}
		/*
		 * if (chunkList != null) {
		 * // TODO: Handle recovered chunks
		 * }
		 */
	}

	@Override
	public byte[][] readAllEntries(final short p_nodeID, final long p_chunkID, final byte p_rangeID,
			final boolean p_manipulateReadPtr) throws DXRAMException {
		byte[][] ret = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			flushDataToPrimaryLog();
			flushDataToSecondaryLogs();

			secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_nodeID, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();

				if (p_manipulateReadPtr) {
					ret = getSecondaryLog(p_chunkID, p_nodeID, p_rangeID).readAll(false);
				} else {
					ret = getSecondaryLog(p_chunkID, p_nodeID, p_rangeID).readAllWithoutReadPtrSet(false);
				}
			}
		} catch (final IOException | InterruptedException e) {}

		return ret;
	}

	@Override
	public void printMetadataOfAllEntries(final short p_owner, final long p_chunkID, final byte p_rangeID)
			throws DXRAMException {
		byte[][] logEntries = null;
		byte[] separatedHeader = null;
		byte[] separatedEntry = null;
		int i = 0;
		int j = 1;
		int readBytes;
		int length;
		int version;
		int offset = 0;
		long localID;
		boolean logStoresMigrations;

		if (p_owner == ChunkID.getCreatorID(p_chunkID)) {
			logStoresMigrations = false;
		} else {
			logStoresMigrations = true;
		}

		try {
			flushDataToPrimaryLog();
			flushDataToSecondaryLogs();
			logEntries = readAllEntries(p_owner, p_chunkID, p_rangeID, false);
		} catch (final IOException | InterruptedException e) {}

		if (logEntries != null) {
			System.out.println("NodeID: " + p_owner);
			while (i < logEntries.length) {
				readBytes = offset;
				offset = 0;
				if (readBytes > 0) {
					// Header was in previous buffer (logEntries[i - 1]), but
					// payload is here
					length = DEFAULT_SEC_LOG_ENTRY_HEADER.getLength(separatedEntry, 0, logStoresMigrations);
					System.arraycopy(logEntries[i], 0, separatedEntry, readBytes,
							DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations)
									+ length - readBytes);
					localID = DEFAULT_SEC_LOG_ENTRY_HEADER.getLID(separatedEntry, 0, logStoresMigrations);
					version = DEFAULT_SEC_LOG_ENTRY_HEADER.getVersion(separatedEntry, 0, logStoresMigrations);
					printMetadata(ChunkID.getCreatorID(p_chunkID), localID, separatedEntry, 0, length,
							version, j++, logStoresMigrations);
					readBytes = length + DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations) - readBytes;
				} else if (readBytes < 0) {
					// A part of the header was in previous buffer (logEntries[i
					// - 1])
					readBytes *= -1;
					System.arraycopy(logEntries[i], 0, separatedHeader, readBytes,
							DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations)
									- readBytes);
					length = DEFAULT_SEC_LOG_ENTRY_HEADER.getLength(separatedHeader, 0, logStoresMigrations);
					localID = DEFAULT_SEC_LOG_ENTRY_HEADER.getLID(separatedHeader, 0, logStoresMigrations);
					version = DEFAULT_SEC_LOG_ENTRY_HEADER.getVersion(separatedEntry, 0, logStoresMigrations);
					readBytes = length + DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations) - readBytes;
					printMetadata(ChunkID.getCreatorID(p_chunkID), localID, logEntries[i], readBytes,
							length, version, j++, logStoresMigrations);
				}

				while (readBytes < logEntries[i].length) {
					if (DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations) > logEntries[i].length
							- readBytes) {
						// Entry is separated: Only a part of the header is in
						// this buffer (logEntries[i])
						separatedHeader = new byte[DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations)];
						System.arraycopy(logEntries[i], readBytes, separatedHeader, 0,
								logEntries[i].length - readBytes);
						offset = -(logEntries[i].length - readBytes);
						readBytes = logEntries[i].length;
					} else {
						length = DEFAULT_SEC_LOG_ENTRY_HEADER.getLength(logEntries[i], readBytes, logStoresMigrations);
						if (length + DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations)
						> logEntries[i].length - readBytes) {
							// Entry is separated: The header is completely in
							// this buffer (logEntries[i])
							separatedEntry =
									new byte[DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations) + length];
							System.arraycopy(logEntries[i], readBytes, separatedEntry, 0, logEntries[i].length
									- readBytes);
							offset = logEntries[i].length - readBytes;
							readBytes = logEntries[i].length;
						} else {
							// Complete entry is in this buffer (logEntries[i])
							localID =
									DEFAULT_SEC_LOG_ENTRY_HEADER.getLID(logEntries[i], readBytes, logStoresMigrations);
							version =
									DEFAULT_SEC_LOG_ENTRY_HEADER.getVersion(logEntries[i], readBytes,
											logStoresMigrations);
							printMetadata(ChunkID.getCreatorID(p_chunkID), localID, logEntries[i],
									readBytes, length, version, j++, logStoresMigrations);
							readBytes += length + DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(logStoresMigrations);
						}
					}
				}
				i++;
			}
		}
	}

	/**
	 * Prints the metadata of one log entry
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LID
	 * @param p_payload
	 *            buffer with payload
	 * @param p_offset
	 *            offset within buffer
	 * @param p_length
	 *            length of payload
	 * @param p_version
	 *            version of chunk
	 * @param p_index
	 *            index of log entry
	 * @param p_logStoresMigrations
	 *            whether the entry is in a secondary log for migrations or not
	 */
	public void printMetadata(final short p_nodeID, final long p_localID, final byte[] p_payload, final int p_offset,
			final int p_length, final int p_version, final int p_index, final boolean p_logStoresMigrations) {
		final long chunkID = ((long) p_nodeID << 48) + p_localID;

		try {
			if (p_version != -1) {
				System.out.println("Log Entry "
						+ p_index
						+ ": \t ChunkID - "
						+ chunkID
						+ "("
						+ p_nodeID
						+ ", "
						+ (int) p_localID
						+ ") \t Length - "
						+ p_length
						+ "\t Version - "
						+ p_version
						+ " \t Payload - "
						+ new String(Arrays.copyOfRange(p_payload,
								p_offset + DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(p_logStoresMigrations),
								p_offset + DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(p_logStoresMigrations)
										+ p_length), "UTF-8"));
			} else {
				System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", "
						+ (int) p_localID + ") \t Length - " + p_length + "\t Version - " + p_version
						+ " \t Tombstones have no payload");
			}
		} catch (final UnsupportedEncodingException | IllegalArgumentException e) {
			System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", "
					+ (int) p_localID + ") \t Length - " + p_length + "\t Version - " + p_version
					+ " \t Payload is no String");
		}
		// p_localID: -1 can only be printed as an int
	}

	@Override
	public PrimaryLog getPrimaryLog() {
		return m_primaryLog;
	}

	@Override
	public SecondaryLogWithSegments getSecondaryLog(final long p_chunkID, final short p_source, final byte p_rangeID)
			throws IOException,
			InterruptedException {
		SecondaryLogWithSegments ret;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.lock();
		if (p_rangeID == -1) {
			cat = m_logCatalogs.get(ChunkID.getCreatorID(p_chunkID) & 0xFFFF);
		} else {
			cat = m_logCatalogs.get(p_source & 0xFFFF);
		}
		ret = cat.getLog(p_chunkID, p_rangeID);
		m_secondaryLogCreationLock.unlock();

		return ret;
	}

	@Override
	public SecondaryLogBuffer getSecondaryLogBuffer(final long p_chunkID, final short p_source, final byte p_rangeID)
			throws IOException,
			InterruptedException {
		SecondaryLogBuffer ret;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.lock();
		if (p_rangeID == -1) {
			cat = m_logCatalogs.get(ChunkID.getCreatorID(p_chunkID) & 0xFFFF);
		} else {
			cat = m_logCatalogs.get(p_source & 0xFFFF);
		}
		ret = cat.getBuffer(p_chunkID, p_rangeID);
		m_secondaryLogCreationLock.unlock();

		return ret;
	}

	@Override
	public long getBackupRange(final long p_chunkID) {
		long ret = -1;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.lock();

		cat = m_logCatalogs.get(ChunkID.getCreatorID(p_chunkID) & 0xFFFF);
		ret = cat.getRange(p_chunkID);
		m_secondaryLogCreationLock.unlock();

		return (p_chunkID & 0xFFFF000000000000L) + ret;
	}

	@Override
	public short getHeaderSize(final short p_nodeID) {
		short ret;

		if (NodeID.getLocalNodeID() == p_nodeID) {
			ret = DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(false);
		} else {
			ret = DEFAULT_SEC_LOG_ENTRY_HEADER.getHeaderSize(true);
		}

		return ret;
	}

	@Override
	public void flushDataToPrimaryLog() throws IOException, InterruptedException {
		m_writeBuffer.signalWriterThreadAndFlushToPrimLog();
	}

	@Override
	public void flushDataToSecondaryLogs() throws IOException, InterruptedException {
		SecondaryLogBuffer[] buffers;

		if (m_flushingInProgress.compareAndSet(false, true)) {
			try {
				for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
					buffers = m_logCatalogs.get(i).getAllBuffers();
					for (int j = 0; j < buffers.length; j++) {
						if (buffers[i] != null && !buffers[i].isBufferEmpty()) {
							buffers[i].flushSecLogBuffer();
						}
					}
				}
			} finally {
				m_flushingInProgress.set(false);
			}
		} else {
			// Another thread is flushing
			do {
				Thread.sleep(LogHandler.FLUSHING_WAITTIME);
			} while (m_flushingInProgress.get());
		}
	}

	/**
	 * Get access to secondary log for reorganization thread
	 * @param p_secLog
	 *            the Secondary Log
	 */
	public void getAccess(final SecondaryLogWithSegments p_secLog) {
		if (!p_secLog.isAccessed()) {
			p_secLog.setAccessFlag(true);
			m_reorgThreadWaits = true;

			while (!m_accessGranted) {
				Thread.yield();
			}
			m_accessGranted = false;
		}
	}

	@Override
	public void grantAccess() {
		if (m_reorgThreadWaits) {
			m_accessGranted = true;
		}
	}

	/**
	 * Handles an incoming LogRequest
	 * @param p_request
	 *            the LogRequest
	 */
	private void incomingLogRequest(final LogRequest p_request) {
		// TODO
	}

	/**
	 * Handles an incoming LogMessage
	 * @param p_message
	 *            the LogMessage
	 */
	private void incomingLogMessage(final LogMessage p_message) {
		final byte rangeID = p_message.getRangeID();
		final short source = p_message.getSource();
		final Chunk[] chunks = p_message.getChunks();

		try {
			for (Chunk chunk : chunks) {
				logChunk(chunk, rangeID, source);
			}
		} catch (final DXRAMException e) {
			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}

	/**
	 * Handles an incoming RemoveMessage
	 * @param p_message
	 *            the RemoveMessage
	 */
	private void incomingRemoveMessage(final RemoveMessage p_message) {
		final byte rangeID = p_message.getRangeID();
		final short source = p_message.getSource();
		final long[] chunkIDs = p_message.getChunkIDs();

		try {
			for (long chunkID : chunkIDs) {
				removeChunk(chunkID, rangeID, source);
			}
		} catch (final DXRAMException e) {
			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}

	/**
	 * Handles an incoming InitRequest
	 * @param p_message
	 *            the InitRequest
	 */
	private void incomingInitRequest(final InitRequest p_message) {
		short owner;
		long firstChunkIDOrRangeID;
		boolean success = true;
		LogCatalog cat;
		SecondaryLogWithSegments secLog = null;

		owner = p_message.getOwner();
		firstChunkIDOrRangeID = p_message.getFirstCIDOrRangeID();

		m_secondaryLogCreationLock.lock();
		cat = m_logCatalogs.get(owner & 0xFFFF);
		if (cat == null) {
			cat = new LogCatalog();
			m_logCatalogs.set(owner & 0xFFFF, cat);
		}
		try {
			if (owner == ChunkID.getCreatorID(firstChunkIDOrRangeID)) {
				// Create new secondary log
				secLog = new SecondaryLogWithSegments(SECLOG_SIZE, m_secondaryLogsReorgThread,
						owner, false);
			} else {
				// Create new secondary log for migrations
				secLog = new SecondaryLogWithSegments(SECLOG_SIZE, m_secondaryLogsReorgThread,
						owner, true);
			}
			// Insert range in log catalog
			cat.insertRange(firstChunkIDOrRangeID, secLog);
		} catch (final IOException | InterruptedException e) {
			System.out.println("ERROR: New range could not be initialized");
			success = false;
		}
		m_secondaryLogCreationLock.unlock();

		try {
			new InitResponse(p_message, success).send(m_network);
		} catch (final NetworkException e) {
			System.out.println("ERROR: Could not acknowledge initilization of backup range");
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {

		if (p_message != null) {
			if (p_message.getType() == LogMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case LogMessages.SUBTYPE_LOG_REQUEST:
					incomingLogRequest((LogRequest) p_message);
					break;
				case LogMessages.SUBTYPE_LOG_MESSAGE:
					incomingLogMessage((LogMessage) p_message);
					break;
				case LogMessages.SUBTYPE_REMOVE_MESSAGE:
					incomingRemoveMessage((RemoveMessage) p_message);
					break;
				case LogMessages.SUBTYPE_INIT_REQUEST:
					incomingInitRequest((InitRequest) p_message);
					break;
				default:
					break;
				}
			}
		}
	}

	@Override
	public void triggerEvent(final ConnectionLostEvent p_event) {
		Contract.checkNotNull(p_event, "no event given");
	}

	// Classes

	/**
	 * Reorganization thread
	 * @author Kevin Beineke 20.06.2014
	 */
	public final class SecondaryLogsReorgThread extends Thread {

		// Attributes
		private VersionsHashTable m_versionsHT;
		private SecondaryLogWithSegments m_secLog;

		// Constructors
		/**
		 * Creates an instance of SecondaryLogsReorgThread
		 */
		public SecondaryLogsReorgThread() {
			m_versionsHT = new VersionsHashTable(6400000, 0.9f);
		}

		/**
		 * Locks the reorganization lock
		 * @note Called before signaling
		 */
		public void lock() {
			m_reorganizationLock.lock();
		}

		/**
		 * Unlocks the reorganization lock
		 * @note Called after signaling
		 */
		public void unlock() {
			m_reorganizationLock.unlock();
		}

		/**
		 * Signals the reorganization thread
		 * @note Called after signaling
		 */
		public void signal() {
			m_thresholdReachedCondition.signal();
		}

		/**
		 * Waits for the reorganization thread to finish reorganization
		 * @throws InterruptedException
		 *             if the thread is interrupted
		 * @note Called after signaling
		 */
		public void await() throws InterruptedException {
			m_reorganizationFinishedCondition.await();
		}

		/**
		 * Determines next log to process
		 * @return secondary log
		 */
		public SecondaryLogWithSegments chooseLog() {
			SecondaryLogWithSegments ret = null;
			long max = 0;
			long current;
			LogCatalog cat;
			SecondaryLogWithSegments[] secLogs;
			SecondaryLogWithSegments secLog;

			for (int i = 0; i < m_logCatalogs.length(); i++) {
				cat = m_logCatalogs.get(i);
				if (cat != null) {
					secLogs = cat.getAllLogs();
					for (int j = 0; j < secLogs.length; j++) {
						secLog = secLogs[j];
						if (secLog != null) {
							current = secLog.getDeleteCounter() * 100 + secLog.getOccupiedSpace();
							if (current > max) {
								max = current;
								ret = secLog;
							}
						}
					}
				}
			}

			return ret;
		}

		/**
		 * Sets the secondary log to reorganize next
		 * @param p_secLog
		 *            the Secondary Log
		 * @note Called before signaling
		 */
		public void setLog(final SecondaryLogWithSegments p_secLog) {
			m_secLog = p_secLog;
		}

		@Override
		public void run() {
			SecondaryLogWithSegments secondaryLog;
			SecondaryLogWithSegments[] secondaryLogs;
			LogCatalog cat;

			while (!m_isShuttingDown) {
				try {
					m_reorganizationLock.lockInterruptibly();
					if (m_isShuttingDown) {
						break;
					}
					secondaryLog = chooseLog();
					if (null != secondaryLog) {
						getAccess(secondaryLog);
						secondaryLog.markInvalidObjects(m_versionsHT);
						for (int i = 0; i < 10; i++) {
							m_writeBuffer.printThroughput();
							if (m_thresholdReachedCondition.await(LogHandler.REORGTHREAD_TIMEOUT,
									TimeUnit.MILLISECONDS) || m_secLog != null) {
								if (m_isShuttingDown) {
									break;
								}
								// Reorganization thread was signaled ->
								// process
								// given log completely
								getAccess(m_secLog);
								m_secLog.markInvalidObjects(new VersionsHashTable(6400000, 0.9f));
								m_secLog.reorganizeAll();
								m_secLog = null;
								m_secLog.setAccessFlag(false);
								m_reorganizationFinishedCondition.signal();
							} else {
								if (m_isShuttingDown) {
									break;
								}
								// Time-out -> reorganize another segment in
								// current log
								getAccess(secondaryLog);
								secondaryLog.reorganizeIteratively();
							}
						}
						secondaryLog.setAccessFlag(false);

						System.out.println(m_primaryLog.getOccupiedSpace() + " bytes in primary log");
						for (int i = 0; i < m_logCatalogs.length(); i++) {
							cat = m_logCatalogs.get(i);
							if (cat != null) {
								System.out.println("Node " + i + ":");
								secondaryLogs = cat.getAllLogs();
								for (int j = 0; j < secondaryLogs.length; j++) {
									System.out.println("Backup range " + j + ":");
									secondaryLog = secondaryLogs[j];
									if (secondaryLog != null) {
										System.out.println(secondaryLog.getOccupiedSpace() + " bytes from " + (short) i
												+ " in secondary log");
										secondaryLog.printSegmentDistribution();
									}
								}
							}
						}
					} else {
						// All secondary logs empty -> sleep
						Thread.sleep(100);
					}
				} catch (final InterruptedException e) {
					System.out.println("Error in reorganization thread: Shutting down!");
					break;
				} finally {
					m_reorganizationLock.unlock();
				}
			}
		}
	}
}
