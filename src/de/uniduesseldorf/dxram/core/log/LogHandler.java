package de.uniduesseldorf.dxram.core.log;


import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.events.LogWriteListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.storage.AbstractLog;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryLog;
import de.uniduesseldorf.dxram.core.log.storage.PrimaryWriteBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogBuffer;
import de.uniduesseldorf.dxram.core.log.storage.SecondaryLogWithSegments;
import de.uniduesseldorf.dxram.core.log.storage.VersionsHashTable;


/**
 * Leads data accesses to a remote node
 * @author Kevin Beineke
 *         29.05.2014
 */
public final class LogHandler implements LogInterface, LogWriteListener {

	// Constants
	public static final int FLASHPAGE_SIZE = 4 * 1024;
	public static final int MAX_NODE_CNT = Short.MAX_VALUE * 2;
	public static final long PRIMARY_LOG_SIZE = Core.getConfiguration().getLongValue(ConfigurationConstants.PRIMARY_LOG_SIZE);
	public static final int PRIMARY_LOG_MIN_SIZE = MAX_NODE_CNT * FLASHPAGE_SIZE;
	public static final long SECONDARY_LOG_SIZE = Core.getConfiguration().getLongValue(ConfigurationConstants.SECONDARY_LOG_SIZE);
	public static final int SECONDARY_LOG_MIN_SIZE = 1024 * FLASHPAGE_SIZE;
	public static final int SEGMENT_SIZE = Core.getConfiguration().getIntValue(ConfigurationConstants.LOG_SEGMENTSIZE);
	public static final int DEFAULT_SECONDARY_LOG_BUFFER_SIZE = FLASHPAGE_SIZE * 2;

	// Must be > 2 * SIGNAL_ON_BYTE_COUNT, e.g. 3 * SIGNAL_ON_BYTE_COUNT
	public static final int WRITE_BUFFER_SIZE = Core.getConfiguration().getIntValue(ConfigurationConstants.WRITE_BUFFER_SIZE);;
	public static final int MAX_WRITE_BUFFER_SIZE = Integer.MAX_VALUE;
	public static final String BACKUP_DIRECTORY = Core.getConfiguration().getStringValue(ConfigurationConstants.LOG_DIRECTORY);
	public static final String PRIMARYLOG_FILENAME = "primary.log";
	public static final String SECLOG_PREFIX_FILENAME = "secondary";
	public static final String SECLOG_POSTFIX_FILENAME = ".log";
	public static final byte[] PRIMLOG_MAGIC = "DXRAMPrimLogv1".getBytes(Charset.forName("UTF-8"));
	public static final byte[] SECLOG_MAGIC = "DXRAMSecLogv1".getBytes(Charset.forName("UTF-8"));

	public static final double REORG_UTILIZATION_THRESHOLD = 70;
	public static final int SIGNAL_ON_BYTE_COUNT = 64 * 1024 * 1024;
	public static final int MAX_BYTE_COUNT = 80 * 1024 * 1024;
	public static final short SECLOGS_REORG_SHUTDOWNTIME = 5;
	public static final long FLUSHING_WAITTIME = 1000L;
	public static final long WRITERTHREAD_TIMEOUTTIME = 500L;
	public static final long REORGTHREAD_TIMEOUT = 1000L;

	public static final byte LOG_HEADER_NID_SIZE = 2;
	public static final byte LOG_HEADER_LID_SIZE = 6;
	public static final byte LOG_HEADER_CID_SIZE = LOG_HEADER_NID_SIZE + LOG_HEADER_LID_SIZE;
	public static final byte LOG_HEADER_LEN_SIZE = 4;
	public static final byte LOG_HEADER_VER_SIZE = 4;
	public static final byte LOG_HEADER_CRC_SIZE = 8;

	public static final byte PRIMARY_HEADER_NID_OFFSET = 0;
	public static final byte PRIMARY_HEADER_LID_OFFSET = LOG_HEADER_NID_SIZE;
	public static final byte PRIMARY_HEADER_CID_OFFSET = 0;
	public static final byte PRIMARY_HEADER_LEN_OFFSET = LOG_HEADER_CID_SIZE;
	public static final byte PRIMARY_HEADER_VER_OFFSET = PRIMARY_HEADER_LEN_OFFSET + LOG_HEADER_LEN_SIZE;
	public static final byte PRIMARY_HEADER_CRC_OFFSET = PRIMARY_HEADER_VER_OFFSET + LOG_HEADER_VER_SIZE;

	public static final byte PRIMARY_HEADER_SIZE = LOG_HEADER_CID_SIZE
			+ LOG_HEADER_LEN_SIZE + LOG_HEADER_VER_SIZE + LOG_HEADER_CRC_SIZE;
	public static final byte SECONDARY_HEADER_SIZE = LOG_HEADER_LID_SIZE
			+ LOG_HEADER_LEN_SIZE + LOG_HEADER_VER_SIZE + LOG_HEADER_CRC_SIZE;
	public static final byte MIN_LOG_ENTRY_SIZE = PRIMARY_HEADER_SIZE + 4;
	public static final byte SECONDARY_TOMBSTONE_SIZE = SECONDARY_HEADER_SIZE;

	public static final int PRIMLOG_HEADER_SIZE = PRIMLOG_MAGIC.length;
	public static final byte READPTR_SIZE = 4;
	public static final byte WRITEPTR_SIZE = 4;
	public static final byte REORGPTR_SIZE = 4;
	public static final int SECLOG_HEADER_SIZE = SECLOG_MAGIC.length + READPTR_SIZE + WRITEPTR_SIZE + REORGPTR_SIZE;

	// Attributes
	private PrimaryWriteBuffer m_writeBuffer;
	private PrimaryLog m_primaryLog;
	private AtomicReferenceArray<SecondaryLogWithSegments> m_secondaryLogs;
	private AtomicReferenceArray<SecondaryLogBuffer> m_secondaryLogBuffers;

	private SecondaryLogsReorgThread m_secondaryLogsReorgThread;
	private Lock m_reorganizationLock;
	private Condition m_reorganizationFinishedCondition;
	private Condition m_thresholdReachedCondition;

	private AtomicBoolean m_flushingInProgress;

	private boolean m_isShuttingDown;


	// Constructors
	/**
	 * Creates an instance of LogHandler
	 */
	public LogHandler() {
		m_writeBuffer = null;
		m_primaryLog = null;
		m_secondaryLogs = null;
		m_secondaryLogBuffers = null;

		m_secondaryLogsReorgThread = null;
		m_reorganizationLock = null;
		m_thresholdReachedCondition = null;

		m_flushingInProgress = null;

		m_isShuttingDown = false;
	}

	// Methods
	@Override
	public void initialize() throws DXRAMException {

		// Create primary log
		try {
			m_primaryLog = new PrimaryLog(PRIMARY_LOG_SIZE);
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error: Primary log creation failed");
		}

		// Create primary log buffer
		m_writeBuffer = new PrimaryWriteBuffer(m_primaryLog, WRITE_BUFFER_SIZE);

		// Create secondary log and secondary log buffer arrays
		m_secondaryLogs = new AtomicReferenceArray<SecondaryLogWithSegments>(LogHandler.MAX_NODE_CNT);
		m_secondaryLogBuffers = new AtomicReferenceArray<SecondaryLogBuffer>(LogHandler.MAX_NODE_CNT);

		// Create reorganization thread for secondary logs
		if (m_secondaryLogsReorgThread == null) {
			m_secondaryLogsReorgThread = new SecondaryLogsReorgThread();
			// Start secondary logs reorganization thread
			m_secondaryLogsReorgThread.start();
		}
		m_reorganizationLock = new ReentrantLock();
		m_reorganizationFinishedCondition = m_reorganizationLock.newCondition();
		m_thresholdReachedCondition = m_reorganizationLock.newCondition();

		m_flushingInProgress = new AtomicBoolean();
		m_flushingInProgress.set(false);
	}

	@Override
	public void close() {
		SecondaryLogBuffer buffer;
		SecondaryLogWithSegments log;

		if (!m_isShuttingDown) {
			m_isShuttingDown = true;

			// Stop reorganization thread
			try {
				m_reorganizationLock.lockInterruptibly();
				m_thresholdReachedCondition.signal();
				m_reorganizationFinishedCondition.await();
			} catch (final InterruptedException e) {
				System.out.println("Could not shut down reorganization thread. Forcing interrupt");
				m_secondaryLogsReorgThread.interrupt();
			} finally {
				m_reorganizationLock.unlock();
			}
			m_secondaryLogsReorgThread = null;
			m_reorganizationFinishedCondition = null;
			m_thresholdReachedCondition = null;
			m_reorganizationLock = null;

			// Clear secondary log buffers
			try {
				for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
					buffer = getSecondaryLogBuffer((short)i, false);
					if (buffer != null) {
						buffer.close();
					}
				}
				m_secondaryLogBuffers = null;
			} catch (final IOException | InterruptedException e) {
				System.out.println("At least one secondary log buffer could not be cleared");
			}
			// Close secondary logs
			try {
				for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
					log = getSecondaryLog((short)i, false);
					if (log != null) {
						log.closeLog();
					}
				}
				m_secondaryLogs = null;
			} catch (final IOException | InterruptedException e) {
				System.out.println("At least one secondary log could not be closed");
			}

			// Close primary log
			if (m_primaryLog != null) {
				try {
					m_primaryLog.closeLog();
				} catch (final InterruptedException | IOException e) {
					System.out.println("Could not close primary log");
				}
				m_primaryLog = null;
			}

			// Close write buffer
			try {
				m_writeBuffer.closeWriteBuffer();
			} catch (final IOException | InterruptedException e) {
				e.printStackTrace();
			}
			m_writeBuffer = null;
		}
	}

	@Override
	public long logChunk(final Chunk p_chunk) throws DXRAMException {
		byte[] logHeader;

		logHeader = AbstractLog.createPrimaryLogEntryHeader(p_chunk);

		//TODO: Remove
		try {
			m_writeBuffer.putLogData(logHeader, p_chunk.getData().array());
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during logging (" + p_chunk.getChunkID() + ")!");
		}


		// TODO: Send and append data


		return 0;
	}

	@Override
	public void removeChunk(final long p_chunkID) throws DXRAMException {
		byte[] tombstone;

		tombstone = AbstractLog.createTombstone(p_chunkID);

		//TODO: Remove
		try {
			m_writeBuffer.putLogData(tombstone, null);
			getSecondaryLog(ChunkID.getCreatorID(p_chunkID), false).incDeleteCounter();
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during deletion (" + p_chunkID + ")!");
		}


		// TODO: Send request
	}

	@Override
	public void recoverAllLogEntries(final short p_nodeID) throws DXRAMException {
		ArrayList<Chunk> chunkList = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			secondaryLogBuffer = getSecondaryLogBuffer(p_nodeID, false);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();

				chunkList = getSecondaryLog(p_nodeID, false).recoverAllLogEntries(true);
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
		ArrayList<Chunk> chunkList = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			secondaryLogBuffer = getSecondaryLogBuffer(p_nodeID, false);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();

				chunkList = getSecondaryLog(p_nodeID, false).recoverRange(true, p_low, p_high);
			}
		} catch (final IOException | InterruptedException e) {
			System.out.println("Error during recovery");
		}
		if (chunkList != null) {
			// TODO: Handle recovered chunks
		}
	}

	@Override
	public byte[][] readAllEntries(final short p_nodeID, final boolean p_manipulateReadPtr) throws DXRAMException {
		byte[][] ret = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			flushDataToPrimaryLog();
			flushDataToSecondaryLogs();

			secondaryLogBuffer = getSecondaryLogBuffer(p_nodeID, false);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();

				if (p_manipulateReadPtr) {
					ret = getSecondaryLog(p_nodeID, false).readAll();
				} else {
					ret = getSecondaryLog(p_nodeID, false).readAllWithoutReadPtrSet();
				}
			}
		} catch (final IOException | InterruptedException e) {}

		return ret;
	}

	@Override
	public void printMetadataOfAllEntries(final short p_nodeID) throws DXRAMException {
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

		//TODO: Fix for use with segments

		try {
			flushDataToPrimaryLog();
			flushDataToSecondaryLogs();
			logEntries = readAllEntries(p_nodeID, false);
		} catch (final IOException | InterruptedException e) {}

		if (logEntries != null) {
			System.out.println("NodeID: " + p_nodeID);
			while (i < logEntries.length) {
				readBytes = offset;
				offset = 0;
				if (readBytes > 0) {
					// Header was in previous buffer (logEntries[i - 1]), but payload is here
					length = AbstractLog.getLengthOfLogEntry(separatedEntry, 0, false);
					System.arraycopy(logEntries[i], 0, separatedEntry,
							readBytes, (SECONDARY_HEADER_SIZE + length) - readBytes);
					localID = AbstractLog.getLIDOfLogEntry(separatedEntry, 0, false);
					version = AbstractLog.getVersionOfLogEntry(separatedEntry, 0, false);
					printMetadata(p_nodeID, localID, separatedEntry, 0, length, version, j++);
					readBytes = (length + SECONDARY_HEADER_SIZE) - readBytes;
				} else if (readBytes < 0) {
					// A part of the header was in previous buffer (logEntries[i - 1])
					readBytes *= -1;
					System.arraycopy(logEntries[i], 0, separatedHeader, readBytes, SECONDARY_HEADER_SIZE - readBytes);
					length = AbstractLog.getLengthOfLogEntry(separatedHeader, 0, false);
					localID = AbstractLog.getLIDOfLogEntry(separatedHeader, 0, false);
					version = AbstractLog.getVersionOfLogEntry(separatedEntry, 0, false);
					readBytes = length + (SECONDARY_HEADER_SIZE - readBytes);
					printMetadata(p_nodeID, localID, logEntries[i], readBytes, length, version, j++);
				}

				while (readBytes < logEntries[i].length) {
					if (SECONDARY_HEADER_SIZE > logEntries[i].length - readBytes) {
						// Entry is separated: Only a part of the header is in this buffer (logEntries[i])
						separatedHeader = new byte[SECONDARY_HEADER_SIZE];
						System.arraycopy(logEntries[i], readBytes, separatedHeader,
								0, logEntries[i].length - readBytes);
						offset = -(logEntries[i].length - readBytes);
						readBytes = logEntries[i].length;
					} else {
						length = AbstractLog.getLengthOfLogEntry(logEntries[i], readBytes, false);
						if (length + SECONDARY_HEADER_SIZE > logEntries[i].length - readBytes) {
							// Entry is separated: The header is completely in this buffer (logEntries[i])
							separatedEntry = new byte[SECONDARY_HEADER_SIZE + length];
							System.arraycopy(logEntries[i], readBytes, separatedEntry,
									0, logEntries[i].length - readBytes);
							offset = logEntries[i].length - readBytes;
							readBytes = logEntries[i].length;
						} else {
							// Complete entry is in this buffer (logEntries[i])
							localID = AbstractLog.getLIDOfLogEntry(logEntries[i], readBytes, false);
							version = AbstractLog.getVersionOfLogEntry(logEntries[i], readBytes, false);
							printMetadata(p_nodeID, localID, logEntries[i], readBytes, length, version, j++);
							readBytes += length + SECONDARY_HEADER_SIZE;
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
	 */
	public void printMetadata(final short p_nodeID, final long p_localID, final byte[] p_payload,
			final int p_offset, final int p_length, final int p_version, final int p_index) {
		final long chunkID = ((long) p_nodeID << 48) + p_localID;

		try {
			System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "("
					+ p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length
					+ "\t Version - " + p_version + " \t Payload - " + new String(
							Arrays.copyOfRange(p_payload, p_offset + SECONDARY_HEADER_SIZE,
									p_offset + SECONDARY_HEADER_SIZE + p_length), "UTF-8"));
		} catch (final UnsupportedEncodingException | IllegalArgumentException e) {
			System.out.println("Log Entry " + p_index + ": \t ChunkID - " + chunkID + "("
					+ p_nodeID + ", " + (int) p_localID + ") \t Length - " + p_length
					+ "\t Version - " + p_version + " \t Payload is no String");
		}
		// p_localID: -1 can only be printed as an int
	}

	@Override
	public PrimaryLog getPrimaryLog() {
		return m_primaryLog;
	}

	@Override
	public SecondaryLogBuffer getSecondaryLogBuffer(final short p_nodeID,
			final boolean p_set) throws IOException, InterruptedException {
		SecondaryLogBuffer ret;
		SecondaryLogWithSegments log;

		ret = m_secondaryLogBuffers.get(p_nodeID & 0xFFFF);
		if (ret == null && p_set) {
			log = new SecondaryLogWithSegments(SECONDARY_LOG_SIZE, m_reorganizationLock,
					m_reorganizationFinishedCondition, m_thresholdReachedCondition, p_nodeID);
			m_secondaryLogs.set(p_nodeID & 0xFFFF, log);

			ret = new SecondaryLogBuffer(log);
			m_secondaryLogBuffers.set(p_nodeID & 0xFFFF, ret);
		}
		return ret;
	}

	@Override
	public SecondaryLogWithSegments getSecondaryLog(final short p_nodeID,
			final boolean p_set) throws IOException, InterruptedException {
		SecondaryLogWithSegments ret;
		SecondaryLogBuffer buffer;

		ret = m_secondaryLogs.get(p_nodeID & 0xFFFF);
		if (ret == null && p_set) {
			ret = new SecondaryLogWithSegments(SECONDARY_LOG_SIZE, m_reorganizationLock,
					m_reorganizationFinishedCondition, m_thresholdReachedCondition, p_nodeID);
			m_secondaryLogs.set(p_nodeID & 0xFFFF, ret);

			buffer = new SecondaryLogBuffer(ret);
			m_secondaryLogBuffers.set(p_nodeID & 0xFFFF, buffer);
		}
		return ret;
	}

	@Override
	public void flushDataToPrimaryLog() throws IOException, InterruptedException {
		m_writeBuffer.singalWriterThreadAndFlushToPrimLog();
	}

	@Override
	public void flushDataToSecondaryLogs() throws IOException, InterruptedException {
		SecondaryLogBuffer secondaryLogBuffer;

		if (m_flushingInProgress.compareAndSet(false, true)) {
			try {
				for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
					secondaryLogBuffer = getSecondaryLogBuffer((short)i, false);
					if (secondaryLogBuffer != null && !secondaryLogBuffer.isBufferEmpty()) {
						secondaryLogBuffer.flushSecLogBuffer();
					}
				}
			} finally {
				m_flushingInProgress.set(false);
			}
		} else {
			// Another thread is flushing
			Thread.sleep(LogHandler.FLUSHING_WAITTIME);
		}
	}

	@Override
	public void triggerEvent(final LogWriteEvent p_event) {
		switch (p_event.getName()) {
		case "PrimLogWrite" :
			p_event.onPrimLogWrite();
			break;
		case "SecLogWrite" :
			p_event.onSecLogWrite();
			break;
		case "WriteBufferExc" :
			p_event.onWriteBufferExc();
			break;
		case "PrimLogExc" :
			p_event.onPrimLogExc();
			break;
		case "SecLogExc" :
			p_event.onSecLogExc();
			break;
		case "SecLogFull" :
			p_event.onSecLogFull();
			break;
		case "ReorgExc" :
			p_event.onReorgExc();
			break;
		case "RestartExc" :
			p_event.onRestartExc();
			break;
		default :
			break;

		}
	}

	// Classes
	/**
	 * Reorganization thread
	 * The reorganization thread reorganizes the secondary logs (threshold or timer)
	 * @author Kevin Beineke
	 *         20.06.2014
	 */
	private final class SecondaryLogsReorgThread extends Thread {
		// Attributes
		private ForkJoinPool m_secLogsReorgFJPool;
		private RecursiveAction m_task;
		private VersionsHashTable m_versionsHT;


		// Constructors
		/**
		 * Creates an instance of SecondaryLogsReorgThread
		 */
		public SecondaryLogsReorgThread() {
			m_secLogsReorgFJPool = new ForkJoinPool();
			m_versionsHT = new VersionsHashTable(6400000, 0.9f);
		}

		/**
		 * Determines next log to process
		 * @return index of log
		 */
		public int chooseLog() {
			int ret = -1;
			long value = 0;
			SecondaryLogWithSegments secLog;

			for (int i = 0; i < m_secondaryLogs.length(); i++) {
				secLog = m_secondaryLogs.get(i);
				if (secLog != null) {
					if (secLog.getDeleteCounter() * 100 + secLog.getOccupiedSpace() > value) {
						value = secLog.getDeleteCounter() * secLog.getOccupiedSpace();
						ret = i;
					}
				}
			}

			return ret;
		}

		/**
		 * Executes the remove task
		 * @param p_logIndex
		 *            the log index
		 * @throws IOException
		 *            if secondary log could not be processed
		 * @throws InterruptedException
		 *            if caller is interrupted
		 */
		public void removeTombstones(final int p_logIndex) throws IOException, InterruptedException {
			SecondaryLogWithSegments secLog = null;

			secLog = m_secondaryLogs.get(p_logIndex);
			m_task = new SecondaryLogWithSegments.RemoveTask(secLog, m_versionsHT);
			m_secLogsReorgFJPool.invoke(m_task);
		}

		/**
		 * Executes the reorganization task
		 * @param p_logIndex
		 *            the log index
		 * @throws IOException
		 *            if secondary log could not be reorganized
		 * @throws InterruptedException
		 *            if caller is interrupted
		 */
		public void reorganizeIteratively(final int p_logIndex) throws IOException, InterruptedException {
			SecondaryLogWithSegments secLog;

			secLog = m_secondaryLogs.get(p_logIndex);
			m_task = new SecondaryLogWithSegments.ReorganizationTask(secLog);
			m_secLogsReorgFJPool.invoke(m_task);
		}

		/**
		 * Executes the reorganization task
		 * @throws IOException
		 *            if secondary log could not be reorganized
		 * @throws InterruptedException
		 *            if caller is interrupted
		 */
		public void reorganizeAll() throws IOException, InterruptedException {
			SecondaryLogWithSegments secLog;
			for (int i = 0; i < m_secondaryLogs.length(); i++) {
				secLog = m_secondaryLogs.get(i);
				if (secLog != null) {
					m_task = new SecondaryLogWithSegments.ReorganizationTask(secLog);
					m_secLogsReorgFJPool.invoke(m_task);
				}
			}
		}

		/**
		 * Closes fork join pool and shuts down reorganization thread
		 */
		private void doShutdown() {
			if (m_isShuttingDown) {
				m_secLogsReorgFJPool.shutdown();
				// Might take some time
				try {
					m_secLogsReorgFJPool.awaitTermination(LogHandler.SECLOGS_REORG_SHUTDOWNTIME, TimeUnit.SECONDS);
				} catch (final InterruptedException e) {}
				m_secLogsReorgFJPool = null;
				m_task = null;
			}
		}

		@Override
		public void run() {
			int logIndex;
			SecondaryLogWithSegments secondaryLog;

			for (;;) {
				try {
					m_reorganizationLock.lockInterruptibly();
					if (m_isShuttingDown) {
						doShutdown();
						break;
					}
					logIndex = chooseLog();
					if (logIndex != -1) {
						removeTombstones(logIndex);
						for (int i = 0; i < 10; i++) {
							if (!m_thresholdReachedCondition.await(
									LogHandler.REORGTHREAD_TIMEOUT, TimeUnit.MILLISECONDS)) {
								reorganizeIteratively(logIndex);
							} else {
								reorganizeAll();
								m_reorganizationFinishedCondition.signal();
							}
						}
						System.out.println(m_primaryLog.getOccupiedSpace() + " bytes in primary log");
						for (int i = 0; i < LogHandler.MAX_NODE_CNT; i++) {
							secondaryLog = getSecondaryLog((short)i, false);
							if (secondaryLog != null) {
								System.out.println(secondaryLog.getOccupiedSpace()
										+ " bytes from " + (short)i + " in secondary log");
								secondaryLog.printSegmentDistribution();
							}
						}
					}
				} catch (final InterruptedException | IOException e) {
					System.out.println("Error: ReorgThread is interrupted. Directly shuting down");
					doShutdown();
					break;
				} finally {
					m_reorganizationLock.unlock();
				}
			}
		}
	}
}
