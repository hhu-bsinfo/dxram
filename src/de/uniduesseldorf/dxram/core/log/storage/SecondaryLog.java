
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.log.LogHandler;

/**
 * This class implements the secondary log
 * @author Kevin Beineke
 *         23.10.2014
 */
public class SecondaryLog extends AbstractLog implements LogStorageInterface {

	// TODO: Three secondary logs per node to accelerate recovery (recover everything from primary)

	// Attributes
	private volatile boolean m_isShuttingDown;

	private long m_secondaryLogReorgThreshold;
	private Lock m_reorganizationLock;
	private Condition m_reorganizationFinishedCondition;
	private Condition m_thresholdReachedCondition;

	private long m_totalUsableSpace;
	private Lock m_secondaryLogLock;

	// Constructors
	/**
	 * Creates an instance of SecondaryLog with default configuration
	 * @param p_reorganizationLock
	 *            the reorganization lock
	 * @param p_thresholdReachedCondition
	 *            the start condition for reorganization
	 * @param p_reorganizationFinishedCondition
	 *            the end condition for reorganization
	 * @param p_nodeID
	 *            the NodeID
	 * @throws IOException
	 *             if secondary could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public SecondaryLog(final Lock p_reorganizationLock, final Condition p_thresholdReachedCondition,
			final Condition p_reorganizationFinishedCondition, final short p_nodeID) throws IOException,
			InterruptedException {
		super(new File(LogHandler.BACKUP_DIRECTORY + "N" + NodeID.getLocalNodeID() + "_"
				+ LogHandler.SECLOG_PREFIX_FILENAME + p_nodeID
				+ LogHandler.SECLOG_POSTFIX_FILENAME), LogHandler.SECONDARY_LOG_SIZE, LogHandler.SECLOG_HEADER_SIZE);

		m_totalUsableSpace = super.getTotalUsableSpace();
		m_secondaryLogLock = new ReentrantLock();

		m_reorganizationLock = p_reorganizationLock;
		m_reorganizationFinishedCondition = p_reorganizationFinishedCondition;
		m_thresholdReachedCondition = p_thresholdReachedCondition;

		m_secondaryLogReorgThreshold = (int)
				(LogHandler.SECONDARY_LOG_SIZE * (LogHandler.REORG_UTILIZATION_THRESHOLD / 100));

		createLogAndWriteHeader();
	}

	/**
	 * Creates an instance of SecondaryLog with default configuration except secondary log size
	 * @param p_secLogSize
	 *            the size of the secondary log
	 * @param p_reorganizationLock
	 *            the reorganization lock
	 * @param p_thresholdReachedCondition
	 *            the start condition for reorganization
	 * @param p_reorganizationFinishedCondition
	 *            the end condition for reorganization
	 * @param p_nodeID
	 *            the NodeID
	 * @throws IOException
	 *             if secondary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public SecondaryLog(final long p_secLogSize, final Lock p_reorganizationLock,
			final Condition p_thresholdReachedCondition, final Condition p_reorganizationFinishedCondition,
			final short p_nodeID) throws IOException, InterruptedException {
		super(new File(LogHandler.BACKUP_DIRECTORY + "N" + NodeID.getLocalNodeID() + "_"
				+ LogHandler.SECLOG_PREFIX_FILENAME + p_nodeID
				+ LogHandler.SECLOG_POSTFIX_FILENAME), p_secLogSize, LogHandler.SECLOG_HEADER_SIZE);
		if (p_secLogSize < LogHandler.SECONDARY_LOG_MIN_SIZE) {
			throw new IllegalArgumentException("Error: Secondary log too small");
		}

		m_totalUsableSpace = super.getTotalUsableSpace();
		m_secondaryLogLock = new ReentrantLock();

		m_reorganizationLock = p_reorganizationLock;
		m_reorganizationFinishedCondition = p_reorganizationFinishedCondition;
		m_thresholdReachedCondition = p_thresholdReachedCondition;

		m_secondaryLogReorgThreshold = (int) (p_secLogSize * (LogHandler.REORG_UTILIZATION_THRESHOLD / 100));

		createLogAndWriteHeader();
	}

	// Methods
	@Override
	public final void closeLog() throws InterruptedException, IOException {
		if (!m_isShuttingDown) {
			m_isShuttingDown = true;
			super.closeRing();
		}
	}

	@Override
	public final int appendData(final byte[] p_data, final int p_offset,
			final int p_length, final Object p_unused) throws IOException, InterruptedException {
		int ret = 0;

		if (p_length <= 0 || p_length > m_totalUsableSpace) {
			throw new IllegalArgumentException("Error: Invalid data size");
		} else if (!m_isShuttingDown) {
			m_secondaryLogLock.lockInterruptibly();
			if (getWritableSpace() >= p_length) {
				ret = appendToLog(p_data, p_offset, p_length);

				// Launch reorganization if threshold is reached
				if (isReorganizationThresholdReached()) {
					System.out.println("Threshold is reached. Calling ReorgThread");
					signalAndWaitSecLogsReorgThread();
				}
			} else {
				System.out.println("Error: Secondary Log full!");
			}
			m_secondaryLogLock.unlock();
		}
		return ret;
	}

	/**
	 * Returns all data of secondary log
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return all data
	 */
	public final byte[][] readAllNodeData() throws IOException, InterruptedException {
		byte[][] result = null;

		try {
			m_secondaryLogLock.lockInterruptibly();
			result = readAll();
		} finally {
			m_secondaryLogLock.unlock();
		}
		return result;
	}

	/**
	 * Returns a list with all log entries wrapped in chunks
	 * @param p_doCRCCheck
	 *            whether to check the payload or not
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public final ArrayList<Chunk> recoverAllLogEntries(final boolean p_doCRCCheck) throws IOException,
			InterruptedException {
		int i = 0;
		int offset = 0;
		int logEntrySize;
		int payloadSize;
		long checksum;
		long chunkID;
		byte[][] logData;
		byte[] payload;
		HashMap<Long, Chunk> chunkMap = null;

		try {
			m_secondaryLogLock.lockInterruptibly();
			logData = readAllWithoutReadPtrSet();
			while (logData[i] != null) {
				chunkMap = new HashMap<Long, Chunk>();
				while (offset + LogHandler.PRIMARY_HEADER_SIZE < logData[i].length) {
					// Determine header of next log entry
					chunkID = getChunkIDOfLogEntry(logData[i], offset);
					payloadSize = getLengthOfLogEntry(logData[i], offset, false);
					checksum = getChecksumOfPayload(logData[i], offset, false);
					logEntrySize = LogHandler.PRIMARY_HEADER_SIZE + payloadSize;

					if (logEntrySize > LogHandler.MIN_LOG_ENTRY_SIZE) {
						// Read payload and create chunk
						if (offset + logEntrySize <= logData[i].length) {
							// Create chunk only if log entry complete
							payload = new byte[payloadSize];
							System.arraycopy(logData[i], offset + LogHandler.PRIMARY_HEADER_SIZE,
									payload, 0, payloadSize);
							if (p_doCRCCheck) {
								if (calculateChecksumOfPayload(payload) != checksum) {
									// Ignore log entry
									offset += logEntrySize;
									continue;
								}
							}
							chunkMap.put(chunkID, new Chunk(chunkID, payload));
						}
					}
					offset += logEntrySize;
				}
				calcAndSetReadPos(offset);
				i++;
			}
		} finally {
			logData = null;
			payload = null;
			m_secondaryLogLock.unlock();
		}
		return (ArrayList<Chunk>) chunkMap.values();
	}

	/**
	 * Returns a list with all log entries wrapped in chunks
	 * @param p_doCRCCheck
	 *            whether to check the payload or not
	 * @param p_low
	 *            lower bound
	 * @param p_high
	 *            higher bound
	 * @throws IOException
	 *             if the secondary log could not be read
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 * @return ArrayList with all log entries as chunks
	 */
	public final ArrayList<Chunk> recoverRange(final boolean p_doCRCCheck,
			final long p_low, final long p_high) throws IOException, InterruptedException {
		int i = 0;
		int offset = 0;
		int logEntrySize;
		int payloadSize;
		long checksum;
		long chunkID;
		long lid;
		byte[][] logData;
		byte[] payload;
		HashMap<Long, Chunk> chunkMap = null;

		try {
			m_secondaryLogLock.lockInterruptibly();
			logData = readAllWithoutReadPtrSet();
			while (logData[i] != null) {
				chunkMap = new HashMap<Long, Chunk>();
				while (offset + LogHandler.PRIMARY_HEADER_SIZE < logData[i].length) {
					// Determine header of next log entry
					payloadSize = getLengthOfLogEntry(logData[i], offset, false);
					logEntrySize = LogHandler.PRIMARY_HEADER_SIZE + payloadSize;
					chunkID = getChunkIDOfLogEntry(logData[i], offset);
					lid = ChunkID.getLocalID(chunkID);
					if (lid >= p_low || lid <= p_high) {
						checksum = getChecksumOfPayload(logData[i], offset, false);

						if (logEntrySize > LogHandler.MIN_LOG_ENTRY_SIZE) {
							// Read payload and create chunk
							if (offset + logEntrySize <= logData[i].length) {
								// Create chunk only if log entry complete
								payload = new byte[payloadSize];
								System.arraycopy(logData[i], offset + LogHandler.PRIMARY_HEADER_SIZE,
										payload, 0, payloadSize);
								if (p_doCRCCheck) {
									if (calculateChecksumOfPayload(payload) != checksum) {
										// Ignore log entry
										offset += logEntrySize;
										continue;
									}
								}
								chunkMap.put(chunkID, new Chunk(chunkID, payload));
							}
						}
					}
					offset += logEntrySize;
				}
				calcAndSetReadPos(offset);
				i++;
			}
		} finally {
			logData = null;
			payload = null;
			m_secondaryLogLock.unlock();
		}
		return (ArrayList<Chunk>) chunkMap.values();
	}

	/**
	 * Checks if the threshold to reorganize is reached
	 * @return whether to reorganize or not
	 */
	public final boolean isReorganizationThresholdReached() {
		boolean ret = false;
		long bytesInRAF;

		bytesInRAF = getOccupiedSpace();
		if (bytesInRAF == 0 || m_isShuttingDown) {
			ret = false;
		} else {
			if (bytesInRAF >= m_secondaryLogReorgThreshold) {
				ret = true;
			} else {
				ret = false;
			}
		}
		return ret;
	}

	/**
	 * Wakes up the reorganization thread
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public final void signalReorganizationThread() throws InterruptedException {

		try {
			m_reorganizationLock.lockInterruptibly();
			m_thresholdReachedCondition.signal();
		} finally {
			// TODO: Check if it is okay to unlock here
			m_reorganizationLock.unlock();
		}
	}

	/**
	 * Wakes up the reorganization thread and waits until reorganization is finished
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public final void signalAndWaitSecLogsReorgThread() throws InterruptedException {

		try {
			m_reorganizationLock.lockInterruptibly();
			m_thresholdReachedCondition.signal();
			m_reorganizationFinishedCondition.await();
		} finally {
			m_reorganizationLock.unlock();
		}
	}

	/**
	 * Reorganization task
	 * The reorganization task
	 * @author Kevin Beineke
	 *         20.06.2014
	 */
	public static final class SecLogReorgTask extends RecursiveAction {
		// Constants
		private static final long serialVersionUID = -6009501638448776535L;

		// Attributes

		// Constructors
		/**
		 * Creates an instance of SecLogReorgTask
		 */
		public SecLogReorgTask() {}

		// Methods
		@Override
		protected void compute() {
			// Flush secondary log buffer
		}
	}
}
