
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.LogHandler;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.log.header.AbstractLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.LogEntryHeaderInterface;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogEntryHeader;
import de.uniduesseldorf.dxram.core.log.header.MigrationPrimLogTombstone;

/**
 * Primary log write buffer Implemented as a ring buffer in a byte array. The
 * in-memory write-buffer for writing on primary log is cyclic. Similar to a
 * ring buffer all read and write accesses are done by using pointers. All
 * readable bytes are between read and write pointer. Unused bytes between write
 * and read pointer. This class is designed for several producers and one
 * consumer (primary log writer- thread).Therefore the write-buffer is
 * implemented thread-safely. There are two write modes. In default mode the
 * buffer is extended adaptively if a threshold is passed (in (flash page size)
 * steps or doubled). Alternatively the caller can be blocked until the write
 * access is completed.
 * @author Kevin Beineke 06.06.2014
 */
public class PrimaryWriteBuffer {

	// Constants
	public static final boolean PARALLEL_BUFFERING = Core.getConfiguration()
			.getBooleanValue(ConfigurationConstants.LOG_PARALLEL_BUFFERING);

	// Attributes
	private LogInterface m_logHandler;

	private byte[] m_buffer;
	private int m_ringBufferSize;
	private PrimaryLogWriterThread m_writerThread;

	private HashMap<Long, Integer> m_lengthByBackupRange;

	private int m_bufferReadPointer;
	private int m_bufferWritePointer;
	private int m_bytesInWriteBuffer;
	private boolean m_isShuttingDown;

	// private AtomicBoolean m_dataAvailable;
	private boolean m_dataAvailable;
	private boolean m_flushingComplete;

	private Semaphore m_metaDataLock;
	private int m_writingNetworkThreads;
	private boolean m_writerThreadWantsToFlush;

	// Constructors
	/**
	 * Creates an instance of PrimaryWriteBuffer with user-specific
	 * configuration
	 * @param p_primaryLog
	 *            Instance of the primary log. Used to write directly to primary
	 *            log if buffer is full
	 * @param p_bufferSize
	 *            size of the ring buffer in bytes (is rounded to a power of
	 *            two)
	 */
	public PrimaryWriteBuffer(final PrimaryLog p_primaryLog,
			final int p_bufferSize) {
		m_bufferReadPointer = 0;
		m_bufferWritePointer = 0;
		m_bytesInWriteBuffer = 0;
		m_writerThread = null;
		m_flushingComplete = false;
		m_dataAvailable = false;

		try {
			m_logHandler = CoreComponentFactory.getLogInterface();
		} catch (final DXRAMException e) {
			System.out.println("Could not get log interface");
		}

		if (p_bufferSize < LogHandler.FLASHPAGE_SIZE
				|| p_bufferSize > LogHandler.WRITE_BUFFER_MAX_SIZE
				|| Integer.bitCount(p_bufferSize) != 1) {
			throw new IllegalArgumentException(
					"Illegal buffer size! Must be 2^x with "
							+ Math.log(LogHandler.FLASHPAGE_SIZE) / Math
									.log(2) + " <= x <= 31");
		} else {
			m_buffer = new byte[p_bufferSize];
			m_ringBufferSize = p_bufferSize;
			m_lengthByBackupRange = new HashMap<Long, Integer>();
			m_isShuttingDown = false;
		}
		m_metaDataLock = new Semaphore(1, false);

		m_writerThread = new PrimaryLogWriterThread();
		m_writerThread.start();
	}

	// Methods
	/**
	 * Cleans the write buffer and resets the pointer
	 * @throws IOException
	 *             if buffer could not be closed
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public final void closeWriteBuffer() throws InterruptedException,
			IOException {
		// Shutdown primary log writer-thread
		m_flushingComplete = false;
		m_isShuttingDown = true;
		while (!m_flushingComplete) {
			Thread.yield();
		}

		m_writerThread = null;
		m_bufferReadPointer = 0;
		m_bufferWritePointer = 0;
		m_ringBufferSize = 0;
		m_buffer = null;
	}

	/**
	 * Print the throughput statistic
	 */
	public void printThroughput() {
		m_writerThread.printThroughput();
	}

	/**
	 * Writes log entries as a whole (max. size: write buffer) Log entry format:
	 * /////// // CID // LEN // CRC// DATA ... ///////
	 * @param p_header
	 *            the log entry's header as a byte array
	 * @param p_payload
	 *            the log entry's payload as a byte array
	 * @throws IOException
	 *             if data could not be flushed to primary log
	 * @throws InterruptedException
	 *             if caller is interrupted
	 * @return the number of written bytes
	 */
	public final int putLogData(final byte[] p_header, final byte[] p_payload)
			throws IOException, InterruptedException {
		LogEntryHeaderInterface logEntryHeader;
		int payloadLength;
		int bytesToWrite;
		int bytesUntilEnd = 0;
		int writePointer;
		Integer counter;
		long rangeID;

		if (p_payload != null) {
			payloadLength = p_payload.length;
		} else {
			payloadLength = 0;
		}

		logEntryHeader = AbstractLogEntryHeader.getPrimaryHeader(p_header, 0);
		if (logEntryHeader instanceof MigrationPrimLogEntryHeader
				|| logEntryHeader instanceof MigrationPrimLogTombstone) {
			rangeID = ((long) -1 << 48) + logEntryHeader.getRangeID(p_header, 0, true);
			bytesToWrite = logEntryHeader.getHeaderSize(true) + payloadLength;
		} else {
			rangeID = m_logHandler.getBackupRange(logEntryHeader.getChunkID(p_header, 0, false));
			bytesToWrite = logEntryHeader.getHeaderSize(false) + payloadLength;
		}

		if (bytesToWrite > m_ringBufferSize) {
			throw new IllegalArgumentException(
					"Data to write exceeds buffer size!");
		}
		if (!m_isShuttingDown) {
			if (PARALLEL_BUFFERING) {
				while (true) {
					m_metaDataLock.acquire();
					if (!m_writerThreadWantsToFlush
							&& m_bytesInWriteBuffer + bytesToWrite <= LogHandler.MAX_BYTE_COUNT) {
						m_writingNetworkThreads++;
						break;
					} else {
						m_metaDataLock.release();
					}
				}
			} else {
				while (m_bytesInWriteBuffer + bytesToWrite > LogHandler.MAX_BYTE_COUNT) {
					Thread.yield();
				}
				m_metaDataLock.acquire();
			}

			// Set buffer write pointer and byte counter before writing to
			// enable multi-threading
			writePointer = m_bufferWritePointer;
			m_bufferWritePointer = writePointer + bytesToWrite;
			if (m_bufferWritePointer >= m_buffer.length) {
				m_bufferWritePointer = bytesToWrite
						- (m_buffer.length - writePointer);
			}
			// Update byte counters
			m_bytesInWriteBuffer += bytesToWrite;

			counter = m_lengthByBackupRange.get(rangeID);
			if (null == counter) {
				m_lengthByBackupRange.put(rangeID, bytesToWrite);
			} else {
				m_lengthByBackupRange.put(rangeID, counter + bytesToWrite);
			}
			if (PARALLEL_BUFFERING) {
				m_metaDataLock.release();
			}

			// Determine free space from end of log to end of array
			if (writePointer >= m_bufferReadPointer) {
				bytesUntilEnd = m_ringBufferSize - writePointer;
			} else {
				bytesUntilEnd = m_bufferReadPointer - writePointer;
			}
			if (bytesToWrite <= bytesUntilEnd) {
				// Write header
				System.arraycopy(p_header, 0, m_buffer, writePointer, p_header.length);
				// Write payload
				if (payloadLength > 0) {
					System.arraycopy(p_payload, 0, m_buffer, writePointer + p_header.length, payloadLength);
				}
			} else {
				// Twofold cyclic write access
				if (bytesUntilEnd < p_header.length) {
					// Write header
					System.arraycopy(p_header, 0, m_buffer, writePointer, bytesUntilEnd);
					System.arraycopy(p_header, bytesUntilEnd, m_buffer, 0, p_header.length - bytesUntilEnd);
					// Write payload
					if (payloadLength > 0) {
						System.arraycopy(p_payload, 0, m_buffer, p_header.length - bytesUntilEnd,
								payloadLength);
					}
				} else if (bytesUntilEnd > p_header.length) {
					// Write header
					System.arraycopy(p_header, 0, m_buffer, writePointer, p_header.length);
					bytesUntilEnd -= p_header.length;
					// Write payload
					if (payloadLength > 0) {
						System.arraycopy(p_payload, 0, m_buffer, writePointer + p_header.length, bytesUntilEnd);
						System.arraycopy(p_payload, bytesUntilEnd, m_buffer, 0, payloadLength - bytesUntilEnd);
					}
				} else {
					// Write header
					System.arraycopy(p_header, 0, m_buffer, writePointer, p_header.length);
					// Write payload
					if (payloadLength > 0) {
						System.arraycopy(p_payload, 0, m_buffer, 0, payloadLength);
					}
				}
			}

			if (PARALLEL_BUFFERING) {
				m_metaDataLock.acquire();
				m_writingNetworkThreads--;
			}

			if (m_bytesInWriteBuffer >= LogHandler.SIGNAL_ON_BYTE_COUNT) {
				// "Wake-up" writer thread if more than SIGNAL_ON_BYTE_COUNT is
				// written
				m_dataAvailable = true;
			}
			m_metaDataLock.release();
		}
		return bytesToWrite;
	}

	/**
	 * Wakes-up writer thread and flushes data to primary log
	 * @throws InterruptedException
	 *             if caller is interrupted
	 */
	public final void signalWriterThreadAndFlushToPrimLog()
			throws InterruptedException {
		m_flushingComplete = false;
		m_dataAvailable = true;

		while (!m_flushingComplete) {
			Thread.yield();
		}
	}

	// Classes
	/**
	 * Writer thread The writer thread flushes data from buffer to primary log
	 * after being waked-up (signal or timer)
	 * @author Kevin Beineke 06.06.2014
	 */
	private final class PrimaryLogWriterThread extends Thread {

		// Attributes
		private long m_time;
		private long m_amount;
		private double m_throughput;

		// Constructors
		/**
		 * Creates an instance of PrimaryLogWriterThread
		 */
		public PrimaryLogWriterThread() {
			m_time = System.currentTimeMillis();
			m_amount = 0;
			m_throughput = 0;
		}

		/**
		 * Print the throughput statistic
		 */
		public void printThroughput() {
			m_throughput = (double) m_amount / (System.currentTimeMillis() - m_time) / 1024 / 1024 * 1000 * 0.9
					+ m_throughput * 0.1;
			m_amount = 0;
			m_time = System.currentTimeMillis();

			System.out.format("Throughput: %.2f mb/s\n", m_throughput);
		}

		@Override
		public void run() {
			long timeStart;

			for (;;) {
				try {
					if (m_isShuttingDown) {
						// Shutdown signal -> directly flush all data to primary
						// log and shut down
						flushDataToPrimaryLog();
						break;
					}

					timeStart = System.currentTimeMillis();
					while (!m_dataAvailable) {
						m_logHandler.grantAccess();
						if (System.currentTimeMillis() > timeStart
								+ LogHandler.WRITERTHREAD_TIMEOUTTIME) {
							// Time-out
							break;
						} else {
							// Wait until enough data is available to flush
							Thread.sleep(10);
							// Thread.yield();
						}
					}
					flushDataToPrimaryLog();
				} catch (final InterruptedException e) {
					System.out
							.println("Error: Writer thread is interrupted. Directly shuting down!");
					break;
				}
			}
		}

		/**
		 * Flushes all data in write buffer to primary log
		 * @return number of copied bytes
		 * @throws InterruptedException
		 *             if caller is interrupted
		 */
		public int flushDataToPrimaryLog() throws InterruptedException {
			int writtenBytes = 0;
			int readPointer;
			int bytesInWriteBuffer;
			Set<Entry<Long, Integer>> lengthByBackupRange;

			// 1. Gain exclusive write access
			// 2. Copy read pointer and counter
			// 3. Set read pointer and reset counter
			// 4. Release lock
			// 5. Write buffer to hard drive
			// -> During writing to hard drive the next slot in Write Buffer can
			// be filled
			if (PARALLEL_BUFFERING) {
				while (true) {
					m_metaDataLock.acquire();
					if (m_writingNetworkThreads == 0) {
						break;
					} else {
						m_writerThreadWantsToFlush = true;
						m_metaDataLock.release();
					}
				}
			} else {
				m_metaDataLock.acquire();
			}

			readPointer = m_bufferReadPointer;
			bytesInWriteBuffer = m_bytesInWriteBuffer;
			lengthByBackupRange = m_lengthByBackupRange.entrySet();

			m_bufferReadPointer = m_bufferWritePointer;
			m_bytesInWriteBuffer = 0;
			m_lengthByBackupRange = new HashMap<Long, Integer>();

			m_dataAvailable = false;
			m_flushingComplete = false;

			m_writerThreadWantsToFlush = false;
			m_metaDataLock.release();

			if (bytesInWriteBuffer > 0) {
				// Write data to primary log
				try {
					writtenBytes = m_logHandler.getPrimaryLog().appendData(
							m_buffer, readPointer, bytesInWriteBuffer,
							lengthByBackupRange);
				} catch (final IOException | InterruptedException e) {
					System.out.println("Error: Could not write to log");
					e.printStackTrace();
				}
			}
			m_amount += writtenBytes;
			m_flushingComplete = true;

			return writtenBytes;
		}
	}
}
