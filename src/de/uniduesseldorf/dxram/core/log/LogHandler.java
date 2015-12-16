
package de.uniduesseldorf.dxram.core.log;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;
import de.uniduesseldorf.dxram.core.log.LogMessages.InitRequest;
import de.uniduesseldorf.dxram.core.log.LogMessages.InitResponse;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogCommandRequest;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogCommandResponse;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.log.header.AbstractLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.DefaultPrimLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.storage.LogCatalog;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryLog;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryWriteBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLog;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogBuffer;
import de.uniduesseldorf.dxram.core.log.storage.VersionsHashTable;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;
import de.uniduesseldorf.dxram.utils.Contract;
import de.uniduesseldorf.dxram.utils.Tools;

/**
 * Leads data accesses to a remote node
 * @author Kevin Beineke 29.05.2014
 */
public final class LogHandler implements LogInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	private static final boolean USE_CHECKSUM = Core.getConfiguration().getBooleanValue(ConfigurationConstants.LOG_CHECKSUM);
	private static final int MAX_NODE_CNT = 65535;
	private static final long FLUSHING_WAITTIME = 1000L;
	private static final long REORGTHREAD_TIMEOUT = 50L;

	private static final AbstractLogEntryHeader DEFAULT_PRIM_LOG_ENTRY_HEADER = new DefaultPrimLogEntryHeader();
	private static final AbstractLogEntryHeader MIGRATION_PRIM_LOG_ENTRY_HEADER = new MigrationPrimLogEntryHeader();

	private static final int PAYLOAD_PRINT_LENGTH = 128;

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
		m_network.register(LogMessage.class, this);
		m_network.register(RemoveMessage.class, this);
		m_network.register(InitRequest.class, this);
		m_network.register(LogCommandRequest.class, this);

		// Create primary log
		try {
			m_primaryLog = new PrimaryLog(this);
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error: Primary log creation failed");
		}

		// Create primary log buffer
		m_writeBuffer = new PrimaryWriteBuffer(this, m_primaryLog);

		// Create secondary log and secondary log buffer catalogs
		m_logCatalogs = new AtomicReferenceArray<LogCatalog>(LogHandler.MAX_NODE_CNT);

		m_secondaryLogCreationLock = new ReentrantLock();

		// Create reorganization thread for secondary logs
		m_secondaryLogsReorgThread = new SecondaryLogsReorgThread();
		m_secondaryLogsReorgThread.setName("Logging: Reorganization Thread");
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
			} catch (final IOException e) {
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
	public short getAproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
		return AbstractLogEntryHeader.getAproxSecLogHeaderSize(NodeID.getLocalNodeID() != p_nodeID, p_localID, p_size);
	}

	@Override
	public void initBackupRange(final long p_firstChunkIDOrRangeID, final short[] p_backupPeers) {
		InitRequest request;
		InitResponse response;
		long time;

		time = System.currentTimeMillis();
		if (null != p_backupPeers) {
			for (int i = 0; i < p_backupPeers.length; i++) {
				if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) != -1) {
					request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID, ChunkID.getCreatorID(p_firstChunkIDOrRangeID));
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
		System.out.println("Time to init range: " + (System.currentTimeMillis() - time));
	}

	@Override
	public Chunk[] recoverBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) throws RecoveryException {
		Chunk[] chunks = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			// TODO: Only flush if there is data for this backup range. Worth it?
			flushDataToPrimaryLog();

			secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();
				chunks = getSecondaryLog(p_chunkID, p_owner, p_rangeID).recoverAllLogEntries(true);
			}
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during recovery");
		}

		return chunks;
	}

	@Override
	public Chunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) throws RecoveryException {
		Chunk[] ret;

		try {
			ret = SecondaryLog.recoverBackupRangeFromFile(p_fileName, p_path);
		} catch (final IOException | InterruptedException e) {
			throw new RecoveryException("Could not recover from file " + p_path + "!");
		}

		return ret;
	}

	@Override
	public void printBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) throws DXRAMException {
		byte[][] segments = null;
		int i = 0;
		int j = 1;
		int readBytes;
		int length;
		int offset = 0;
		long chunkID;
		EpochVersion version;
		AbstractLogEntryHeader logEntryHeader;

		segments = readBackupRange(p_owner, p_chunkID, p_rangeID);
		if (segments != null) {
			System.out.println();
			System.out.println("NodeID: " + p_owner);
			while (segments[i] != null) {
				System.out.println("Segment " + i + ": " + segments[i].length);
				readBytes = offset;
				offset = 0;
				while (readBytes < segments[i].length) {
					logEntryHeader = AbstractLogEntryHeader.getSecondaryHeader(segments[i], readBytes, p_owner != ChunkID.getCreatorID(p_chunkID));
					chunkID = logEntryHeader.getCID(segments[i], readBytes);
					length = logEntryHeader.getLength(segments[i], readBytes);
					version = logEntryHeader.getVersion(segments[i], readBytes);
					printMetadata(ChunkID.getCreatorID(p_chunkID), chunkID, segments[i], readBytes, length, version, j++, logEntryHeader);
					readBytes += length + logEntryHeader.getHeaderSize(segments[i], readBytes);
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
	 *            the LocalID
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
	 * @param p_logEntryHeader
	 *            the log entry header
	 */
	private void printMetadata(final short p_nodeID, final long p_localID, final byte[] p_payload, final int p_offset, final int p_length,
			final EpochVersion p_version, final int p_index, final AbstractLogEntryHeader p_logEntryHeader) {
		final long chunkID = ((long) p_nodeID << 48) + p_localID;
		byte[] array;

		try {
			if (p_version.getVersion() != -1) {
				array =
						new String(Arrays.copyOfRange(p_payload, p_offset + p_logEntryHeader.getHeaderSize(p_payload, p_offset), p_offset
								+ p_logEntryHeader.getHeaderSize(p_payload, p_offset) + PAYLOAD_PRINT_LENGTH)).trim().getBytes();

				if (Tools.looksLikeUTF8(array)) {
					System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", " + (int) p_localID + ") \t Length - "
							+ p_length + "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion() + " \t Payload - " + new String(array, "UTF-8"));
				} else {
					System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", " + (int) p_localID + ") \t Length - "
							+ p_length + "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion() + " \t Payload is no String");
				}
			} else {
				System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length
						+ "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion() + " \t Tombstones have no payload");
			}
		} catch (final UnsupportedEncodingException | IllegalArgumentException e) {
			System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "(" + p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length
					+ "\t Version - " + p_version.getEpoch() + "," + p_version.getVersion() + " \t Payload is no String");
		}
		// p_localID: -1 can only be printed as an int
	}

	/**
	 * Reads the local data of one log
	 * @param p_owner
	 *            the NodeID
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @throws DXRAMException
	 *             if the Chunks could not be read
	 * @return the local data
	 * @note for testing only
	 */
	private byte[][] readBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) throws DXRAMException {
		byte[][] ret = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			flushDataToPrimaryLog();
			flushDataToSecondaryLogs();

			secondaryLogBuffer = getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();

				ret = getSecondaryLog(p_chunkID, p_owner, p_rangeID).readAllSegments();
			}
		} catch (final IOException | InterruptedException e) {}

		return ret;
	}

	/**
	 * Returns the backup range
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the first ChunkID of the range
	 */
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

	/**
	 * Returns the secondary log
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_source
	 *            the source NodeID
	 * @param p_rangeID
	 *            the RangeID for migrations or -1
	 * @return the secondary log
	 * @throws IOException
	 *             if the secondary log could not be returned
	 * @throws InterruptedException
	 *             if the secondary log could not be returned
	 */
	private SecondaryLog getSecondaryLog(final long p_chunkID, final short p_source,
			final byte p_rangeID) throws IOException, InterruptedException {
		SecondaryLog ret;
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

	/**
	 * Returns the secondary log buffer
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_source
	 *            the source NodeID
	 * @param p_rangeID
	 *            the RangeID for migrations or -1
	 * @return the secondary log buffer
	 * @throws IOException
	 *             if the secondary log buffer could not be returned
	 * @throws InterruptedException
	 *             if the secondary log buffer could not be returned
	 */
	public SecondaryLogBuffer getSecondaryLogBuffer(final long p_chunkID, final short p_source, final byte p_rangeID) throws IOException, InterruptedException {
		SecondaryLogBuffer ret = null;
		LogCatalog cat;

		// Can be executed by application/network thread or writer thread
		m_secondaryLogCreationLock.lock();
		if (p_rangeID == -1) {
			cat = m_logCatalogs.get(ChunkID.getCreatorID(p_chunkID) & 0xFFFF);
		} else {
			cat = m_logCatalogs.get(p_source & 0xFFFF);
		}

		if (cat != null) {
			ret = cat.getBuffer(p_chunkID, p_rangeID);
		}
		m_secondaryLogCreationLock.unlock();

		return ret;
	}

	/**
	 * Flushes the primary log write buffer
	 * @throws IOException
	 *             if primary log could not be flushed
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	private void flushDataToPrimaryLog() throws IOException, InterruptedException {
		m_writeBuffer.signalWriterThreadAndFlushToPrimLog();
	}

	/**
	 * Flushes all secondary log buffers
	 * @throws IOException
	 *             if at least one secondary log could not be flushed
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public void flushDataToSecondaryLogs() throws IOException, InterruptedException {
		LogCatalog cat;
		SecondaryLogBuffer[] buffers;

		if (m_flushingInProgress.compareAndSet(false, true)) {
			try {
				for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
					cat = m_logCatalogs.get(i);
					if (cat != null) {
						buffers = cat.getAllBuffers();
						for (int j = 0; j < buffers.length; j++) {
							if (buffers[j] != null && !buffers[j].isBufferEmpty()) {
								buffers[j].flushSecLogBuffer();
							}
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
	private void getAccessToSecLog(final SecondaryLog p_secLog) {
		if (!p_secLog.isAccessed()) {
			p_secLog.setAccessFlag(true);
			m_reorgThreadWaits = true;

			while (!m_accessGranted) {
				Thread.yield();
			}
			m_accessGranted = false;
		}
	}

	/**
	 * Get access to secondary log for reorganization thread
	 * @param p_secLog
	 *            the Secondary Log
	 */
	private void leaveSecLog(final SecondaryLog p_secLog) {
		if (p_secLog.isAccessed()) {
			p_secLog.setAccessFlag(false);
		}
	}

	/**
	 * Grants the reorganization thread access to a secondary log
	 */
	public void grantAccess() {
		if (m_reorgThreadWaits) {
			m_accessGranted = true;
		}
	}

	/**
	 * Returns the current utilization of primary log and all secondary logs
	 * @return the current utilization
	 */
	private String getCurrentUtilization() {
		String ret;
		int counter;
		SecondaryLog[] secondaryLogs;
		SecondaryLogBuffer[] secLogBuffers;
		LogCatalog cat;

		ret = "***********************************************************************\n"
				+ "*Primary log: " + m_primaryLog.getOccupiedSpace() + " bytes\n"
				+ "***********************************************************************\n\n"
				+ "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
				+ "+Secondary logs:\n";

		for (int i = 0; i < m_logCatalogs.length(); i++) {
			cat = m_logCatalogs.get(i);
			if (cat != null) {
				counter = 0;
				ret += "++Node " + (short) i + ":\n";
				secondaryLogs = cat.getAllCreatorLogs();
				secLogBuffers = cat.getAllCreatorBuffers();
				for (int j = 0; j < secondaryLogs.length; j++) {
					ret += "+++Creator backup range " + j + ": ";
					if (secondaryLogs[j] != null) {
						if (secondaryLogs[j].isAccessed()) {
							ret += "#Active log# ";
						}
						ret += secondaryLogs[j].getOccupiedSpace() + " bytes (in buffer: " + secLogBuffers[j].getOccupiedSpace() + " bytes)\n";
						ret += secondaryLogs[j].getSegmentDistribution() + "\n";
						counter += secondaryLogs[j].getOccupiedSpace();
					}
				}
				secondaryLogs = cat.getAllMigrationLogs();
				secLogBuffers = cat.getAllMigrationBuffers();
				for (int j = 0; j < secondaryLogs.length; j++) {
					ret += "+++Migration backup range " + j + ": ";
					if (secondaryLogs[j] != null) {
						if (secondaryLogs[j].isAccessed()) {
							ret += "#Active log# ";
						}
						ret += secondaryLogs[j].getOccupiedSpace() + " bytes (in buffer: " + secLogBuffers[j].getOccupiedSpace() + " bytes)\n";
						ret += secondaryLogs[j].getSegmentDistribution() + "\n";
						counter += secondaryLogs[j].getOccupiedSpace();
					}
				}
				ret += "++Bytes per node: " + counter + "\n";
			}
		}
		ret += "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n";

		return ret;
	}

	/**
	 * Handles an incoming LogMessage
	 * @param p_message
	 *            the LogMessage
	 */
	private void incomingLogMessage(final LogMessage p_message) {
		long chunkID;
		int length;
		int position;
		byte[] logEntryHeader;
		byte[] payload;
		final ByteBuffer buffer = p_message.getMessageBuffer();
		final short source = p_message.getSource();
		final byte rangeID = buffer.get();
		final int size = buffer.getInt();
		SecondaryLog secLog;

		for (int i = 0; i < size; i++) {
			chunkID = buffer.getLong();
			length = buffer.getInt();

			assert length > 0;

			try {
				secLog = getSecondaryLog(chunkID, source, rangeID);
				if (USE_CHECKSUM) {
					// Get the payload for calculating the checksum (position is not adjusted)
					position = buffer.position();
					payload = new byte[length];
					buffer.get(payload, 0, length);
					buffer.position(position);

					if (rangeID == -1) {
						logEntryHeader = DEFAULT_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length,
								secLog.getNextVersion(chunkID), payload, (byte) -1, (short) -1);
					} else {
						logEntryHeader = MIGRATION_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length,
								secLog.getNextVersion(chunkID), payload, rangeID, source);
					}
				} else {
					if (rangeID == -1) {
						logEntryHeader = DEFAULT_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length,
								secLog.getNextVersion(chunkID), null, (byte) -1, (short) -1);
					} else {
						logEntryHeader = MIGRATION_PRIM_LOG_ENTRY_HEADER.createLogEntryHeader(chunkID, length,
								secLog.getNextVersion(chunkID), null, rangeID, source);
					}
				}

				m_writeBuffer.putLogData(logEntryHeader, buffer, length);
				// TODO: Increase invalid counter at the end of an epoch
				/*-if (version > 1) {
					secLog.incLogInvalidCounter();
				}*/
			} catch (final IOException | InterruptedException e) {
				System.out.println("Error during logging (" + chunkID + ")!");
			}
		}
	}

	/**
	 * Handles an incoming RemoveMessage
	 * @param p_message
	 *            the RemoveMessage
	 */
	private void incomingRemoveMessage(final RemoveMessage p_message) {
		long chunkID;
		final ByteBuffer buffer = p_message.getMessageBuffer();
		final short source = p_message.getSource();
		final byte rangeID = buffer.get();
		final int size = buffer.getInt();

		for (int i = 0; i < size; i++) {
			chunkID = buffer.getLong();

			try {
				getSecondaryLog(chunkID, source, rangeID).invalidateChunk(chunkID);
			} catch (final IOException | InterruptedException e) {
				System.out.println("Error during deletion (" + chunkID + ")!");
			}
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
		SecondaryLog secLog = null;

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
				secLog = new SecondaryLog(m_secondaryLogsReorgThread, owner, ChunkID.getLocalID(firstChunkIDOrRangeID), cat.getNewID(false), false);
			} else {
				// Create new secondary log for migrations
				secLog = new SecondaryLog(m_secondaryLogsReorgThread, owner, firstChunkIDOrRangeID, cat.getNewID(true), true);
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

	/**
	 * Handles an incoming LogCommandRequest
	 * @param p_request
	 *            the CommandRequest
	 */
	private void incomingCommandRequest(final LogCommandRequest p_request) {
		String res;

		if (p_request.getArgument().contains("loginfo")) {
			res = getCurrentUtilization();
		} else {
			res = "error";
		}

		try {
			new LogCommandResponse(p_request, res).send(m_network);
		} catch (final NetworkException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {

		if (p_message != null) {
			if (p_message.getType() == LogMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case LogMessages.SUBTYPE_LOG_MESSAGE:
					incomingLogMessage((LogMessage) p_message);
					break;
				case LogMessages.SUBTYPE_REMOVE_MESSAGE:
					incomingRemoveMessage((RemoveMessage) p_message);
					break;
				case LogMessages.SUBTYPE_INIT_REQUEST:
					incomingInitRequest((InitRequest) p_message);
					break;
				case LogMessages.SUBTYPE_LOG_COMMAND_REQUEST:
					incomingCommandRequest((LogCommandRequest) p_message);
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
		private SecondaryLog m_secLog;

		// Constructors
		/**
		 * Creates an instance of SecondaryLogsReorgThread
		 */
		public SecondaryLogsReorgThread() {
			m_versionsHT = new VersionsHashTable(6400000, 0.9f);
		}

		// Setter
		/**
		 * Sets the secondary log to reorganize next
		 * @param p_secLog
		 *            the Secondary Log
		 * @note Called before signaling
		 */
		public void setLog(final SecondaryLog p_secLog) {
			m_secLog = p_secLog;
		}

		// Methods
		/**
		 * Locks the reorganization lock
		 * @note Called before signaling
		 */
		public void lock() {
			// Grant access for reorganization thread to avoid deadlock
			grantAccess();

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
		private SecondaryLog chooseLog() {
			SecondaryLog ret = null;
			long max = 0;
			long current;
			LogCatalog cat;
			ArrayList<LogCatalog> cats;
			SecondaryLog[] secLogs;
			SecondaryLog secLog;

			cats = new ArrayList<LogCatalog>();
			for (int i = 0; i < m_logCatalogs.length(); i++) {
				cat = m_logCatalogs.get(i);
				if (cat != null) {
					cats.add(cat);
				}
			}

			for (LogCatalog currentCat : cats) {
				secLogs = currentCat.getAllLogs();
				for (int j = 0; j < secLogs.length; j++) {
					secLog = secLogs[j];
					if (secLog != null) {
						current = secLog.getLogInvalidCounter();
						if (current > max) {
							max = current;
							ret = secLog;
						}
					}
				}
			}
			if (ret == null && !cats.isEmpty()) {
				// Choose one secondary log randomly
				cat = cats.get(Tools.getRandomValue(cats.size() - 1));
				secLogs = cat.getAllLogs();
				if (secLogs.length > 0) {
					ret = secLogs[Tools.getRandomValue(secLogs.length - 1)];
				}
			}

			return ret;
		}

		@Override
		public void run() {
			SecondaryLog secondaryLog;

			while (!m_isShuttingDown) {
				try {
					m_reorganizationLock.lockInterruptibly();
					if (m_isShuttingDown) {
						break;
					}

					secondaryLog = chooseLog();
					if (null != secondaryLog) {
						getAccessToSecLog(secondaryLog);
						if (secondaryLog.getLogInvalidCounter() != 0) {
							secondaryLog.getCurrentVersions(m_versionsHT);
						}
						for (int i = 0; i < 10; i++) {
							// m_writeBuffer.printThroughput();
							if (m_thresholdReachedCondition.await(LogHandler.REORGTHREAD_TIMEOUT, TimeUnit.MILLISECONDS) || m_secLog != null) {
								if (m_isShuttingDown) {
									break;
								}
								// Reorganization thread was signaled -> process given log completely
								getAccessToSecLog(m_secLog);
								m_secLog.getCurrentVersions(new VersionsHashTable(6400000, 0.9f));
								m_secLog.reorganizeAll();
								leaveSecLog(m_secLog);
								m_secLog = null;
								m_reorganizationFinishedCondition.signal();
								break;
							} else {
								if (m_isShuttingDown) {
									break;
								}
								// Time-out -> reorganize another segment in current log
								getAccessToSecLog(secondaryLog);
								secondaryLog.reorganizeIteratively();
							}
						}
						leaveSecLog(secondaryLog);
					} else {
						// All secondary logs empty -> sleep
						Thread.sleep(100);
					}
					m_reorganizationLock.unlock();
				} catch (final InterruptedException e) {
					System.out.println("Error in reorganization thread: Shutting down!");
					if (m_reorganizationLock != null && m_reorganizationLock.tryLock()) {
						m_reorganizationLock.unlock();
					}
					break;
				}
			}
		}
	}

}
