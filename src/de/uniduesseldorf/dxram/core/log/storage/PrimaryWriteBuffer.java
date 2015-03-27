package de.uniduesseldorf.dxram.core.log.storage;

import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.log.LogHandler;
import de.uniduesseldorf.dxram.core.log.LogInterface;

import sun.misc.Unsafe;


/**
 * Primary log write buffer
 * Implemented as a ring buffer in a byte array.
 * The in-memory write-buffer for writing on primary log is cyclic. Similar to a
 * ring buffer all read and write accesses are done by using pointers. All readable
 * bytes are between read and write pointer. Unused bytes between write and read
 * pointer.
 * This class is designed for several producers and one consumer (primary log writer-
 * thread).Therefore the write-buffer is implemented thread-safely. There are two write
 * modes. In default mode the buffer is extended adaptively if a threshold is passed (in
 * (flash page size) steps or doubled). Alternatively the caller can be blocked until the
 * write access is completed.
 * @author Kevin Beineke
 *         06.06.2014
 */
public class PrimaryWriteBuffer {

	// Constants
	private static final int THREADS = 32;

	// Attributes
	private LogInterface m_logHandler;

	private byte[] m_buffer;
	private int m_ringBufferSize;
	private PrimaryLogWriterThread m_writerThread;

	private int[] m_lengthByNode;

	private int m_bufferReadPointer;
	private int m_bufferWritePointer;
	private int m_bytesInWriteBuffer;
	private boolean m_isShuttingDown;

	//private AtomicBoolean m_dataAvailable;
	private boolean m_dataAvailable;
	private boolean m_flushingComplete;

	private Lock m_metaDataLock;
	private int m_tickets;
	private boolean m_blocked;



	// Constructors
	/**
	 * Creates an instance of PrimaryWriteBuffer with default configuration
	 * @param p_primaryLog
	 *            Instance of the primary log. Used to write directly to primary log if buffer is full
	 */
	public PrimaryWriteBuffer(final PrimaryLog p_primaryLog){
		this (p_primaryLog, LogHandler.WRITE_BUFFER_SIZE);
	}

	/**
	 * Creates an instance of PrimaryWriteBuffer with user-specific configuration
	 * @param p_primaryLog
	 *            Instance of the primary log. Used to write directly to primary log if buffer is full
	 * @param p_bufferSize
	 *            size of the ring buffer in bytes (is rounded to a power of two)
	 */
	public PrimaryWriteBuffer(final PrimaryLog p_primaryLog, final int p_bufferSize) {
		int bufferSize = p_bufferSize;
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

		if (bufferSize < (LogHandler.FLASHPAGE_SIZE) || bufferSize > LogHandler.MAX_WRITE_BUFFER_SIZE) {
			throw new IllegalArgumentException("Illegal buffer size!");
		} else {
			if (Integer.bitCount(bufferSize) != 1) {
				bufferSize = (int) (1 << (32 - Integer.numberOfLeadingZeros(bufferSize - 1)));
			}
			m_buffer = new byte[bufferSize];
			m_ringBufferSize = bufferSize;
			m_lengthByNode = new int[Short.MAX_VALUE * 2];
			m_isShuttingDown = false;
		}
		m_metaDataLock = new ReentrantLock(false);

		if (m_writerThread == null) {
			m_writerThread = new PrimaryLogWriterThread();
			m_writerThread.start();
			try {
				// Wait for consumer thread to be ready
				Thread.sleep(100L);
			} catch (final InterruptedException e) {}
		}
	}

	// Getter
	/**
	 * Returns consumer
	 * @return consumer
	 */
	public final PrimaryLogWriterThread getConsumer(){
		return m_writerThread;
	}

	// Setter
	/**
	 * Sets consumer
	 * @param p_consumer
	 *            the consumer
	 */
	public final void setConsumer(final PrimaryLogWriterThread p_consumer) {
		m_writerThread = p_consumer;
	}


	// Methods
	/**
	 * Cleans the write buffer and resets the pointer
	 * @throws IOException
	 *            if buffer could not be closed
	 * @throws InterruptedException
	 *            if caller is interrupted
	 */
	public final void closeWriteBuffer() throws InterruptedException, IOException {
		if (!m_isShuttingDown) {
			// To shutdown primary log writer-threads
			m_isShuttingDown = true;
			m_flushingComplete = false;

			// Wait for writer thread
			while (!m_flushingComplete) {
				Thread.yield();
			}

			m_writerThread = null;
			m_bufferReadPointer = 0;
			m_bufferWritePointer = 0;
			m_ringBufferSize = 0;
			m_buffer = null;
		}
	}

	/**
	 * Writes log entries as a whole (max. size: write buffer)
	 * Log entry format: //////////////////////////////////
	 *                   // OID // LEN // CRC// DATA ... //
	 *                   //////////////////////////////////
	 * @param p_header
	 *            the log entry's header as a byte array
	 * @param p_payload
	 *            the log entry's payload as a byte array
	 * @throws IOException
	 *            if data could not be flushed to primary log
	 * @throws InterruptedException
	 *            if caller is interrupted
	 * @return the number of written bytes
	 */
	public final int putLogData(final byte[] p_header, final byte[] p_payload) throws
	IOException, InterruptedException {
		int payloadLength;
		int bytesToWrite;
		int bytesUntilEnd = 0;
		int writePointer;

		if (p_payload != null) {
			payloadLength = p_payload.length;
		} else {
			payloadLength = 0;
		}
		bytesToWrite = LogHandler.PRIMARY_HEADER_SIZE + payloadLength;

		if (p_header.length != LogHandler.PRIMARY_HEADER_SIZE) {
			throw new IllegalArgumentException("The header is corrupted.");
		}
		if (bytesToWrite > m_ringBufferSize) {
			throw new IllegalArgumentException("Data to write exceeds buffer size!");
		}
		if (!m_isShuttingDown) {
			/*while (true) {
				m_metaDataLock.lock();
				if (!m_blocked && (m_bytesInWriteBuffer + bytesToWrite <= LogHandler.MAX_BYTE_COUNT)) {
					m_tickets++;
					break;
				} else {
					m_metaDataLock.unlock();
				}
			}*/
			while (m_bytesInWriteBuffer + bytesToWrite > LogHandler.MAX_BYTE_COUNT) {Thread.yield();}
			m_metaDataLock.lock();

			// Set buffer write pointer and byte counter before writing to enable multi-threading
			writePointer = m_bufferWritePointer;
			m_bufferWritePointer = writePointer + bytesToWrite;
			if (m_bufferWritePointer >= m_buffer.length) {
				m_bufferWritePointer = bytesToWrite - (m_buffer.length - writePointer);
			}
			// Update byte counter
			m_bytesInWriteBuffer += bytesToWrite;
			m_lengthByNode[AbstractLog.getNodeIDOfLogEntry(p_header, 0) & 0xFFFF] += bytesToWrite;
			//m_metaDataLock.unlock();


			// Determine free space from end of log to end of array
			if (writePointer >= m_bufferReadPointer) {
				bytesUntilEnd = m_ringBufferSize - writePointer;
			} else {
				bytesUntilEnd = m_bufferReadPointer - writePointer;
			}
			if (bytesToWrite <= bytesUntilEnd) {
				// Write header
				System.arraycopy(p_header, 0, m_buffer, writePointer, LogHandler.PRIMARY_HEADER_SIZE);
				// Write payload
				if (payloadLength > 0) {
					System.arraycopy(p_payload, 0,
							m_buffer, writePointer + LogHandler.PRIMARY_HEADER_SIZE, payloadLength);
				}
			} else {
				// Twofold cyclic write access
				if (bytesUntilEnd < LogHandler.PRIMARY_HEADER_SIZE) {
					// Write header
					System.arraycopy(p_header, 0, m_buffer, writePointer, bytesUntilEnd);
					System.arraycopy(p_header, bytesUntilEnd, m_buffer, 0,
							LogHandler.PRIMARY_HEADER_SIZE - bytesUntilEnd);
					// Write payload
					if (payloadLength > 0) {
						System.arraycopy(p_payload, 0, m_buffer,
								LogHandler.PRIMARY_HEADER_SIZE - bytesUntilEnd, payloadLength);
					}
				} else if (bytesUntilEnd > LogHandler.PRIMARY_HEADER_SIZE) {
					// Write header
					System.arraycopy(p_header, 0, m_buffer, writePointer, LogHandler.PRIMARY_HEADER_SIZE);
					bytesUntilEnd -= LogHandler.PRIMARY_HEADER_SIZE;
					// Write payload
					if (payloadLength > 0) {
						System.arraycopy(p_payload, 0,
								m_buffer, writePointer + LogHandler.PRIMARY_HEADER_SIZE, bytesUntilEnd);
						System.arraycopy(p_payload, bytesUntilEnd, m_buffer, 0, payloadLength - bytesUntilEnd);
					}
				} else {
					// Write header
					System.arraycopy(p_header, 0, m_buffer, writePointer, LogHandler.PRIMARY_HEADER_SIZE);
					// Write payload
					if (payloadLength > 0) {
						System.arraycopy(p_payload, 0, m_buffer, 0, payloadLength);
					}
				}
			}

			/*m_metaDataLock.lock();
			m_tickets--;*/

			if (m_bytesInWriteBuffer >= LogHandler.SIGNAL_ON_BYTE_COUNT && !m_dataAvailable) {
				// "Wake-up" writer thread if more than SIGNAL_ON_BYTE_COUNT is written
				m_dataAvailable = true;
			}
			m_metaDataLock.unlock();
		}
		return bytesToWrite;
	}

	/**
	 * Wakes-up writer thread and flushes data to primary log
	 * @throws InterruptedException
	 *            if caller is interrupted
	 */
	public final void singalWriterThreadAndFlushToPrimLog() throws InterruptedException {
		m_dataAvailable = true;
		m_flushingComplete = false;

		while (!m_flushingComplete) {
			Thread.yield();
		}
	}

	/**
	 * Determines the number of free bytes in buffer
	 * @return remaining bytes
	 */
	public final int determineWritableSpace(){
		return m_ringBufferSize - m_bytesInWriteBuffer;
	}


	// Classes
	/**
	 * Writer thread
	 * The writer thread flushes data from buffer to primary log after being waked-up (signal or timer)
	 * @author Kevin Beineke
	 *         06.06.2014
	 */
	private final class PrimaryLogWriterThread extends Thread {

		// Constructors
		/**
		 * Creates an instance of PrimaryLogWriterThread
		 */
		public PrimaryLogWriterThread(){}

		@Override
		public void run() {
			long timeStart;

			for (;;) {
				try {
					if (m_isShuttingDown) {
						// Shutdown signal -> directly flush all data to primary log and shut down
						flushDataToPrimaryLog();
						break;
					}

					timeStart = System.currentTimeMillis();
					while (!m_dataAvailable) {
						if (System.currentTimeMillis() > timeStart + LogHandler.WRITERTHREAD_TIMEOUTTIME) {
							// Time-out
							break;
						} else {
							// Wait until enough data is available to flush
							Thread.sleep(10);
							//Thread.yield();
						}
					}

					flushDataToPrimaryLog();
				} catch (final InterruptedException e) {
					System.out.println("Error: PrimLogWriter is interrupted. Directly shuting down");
					break;
				}
			}
		}

		/**
		 * Flushes all data in write buffer to primary log
		 * @return number of copied bytes
		 * @throws InterruptedException
		 *            if caller is interrupted
		 */
		public int flushDataToPrimaryLog() throws InterruptedException {
			int writtenBytes = 0;
			int readPointer;
			int bytesInWriteBuffer;
			int[] lengthByNode;

			// 1. Gain exclusive write access
			// 2. Copy read pointer and counter
			// 3. Set read pointer and reset counter
			// 4. Release lock
			// 5. Write buffer to hard drive
			// -> During writing to hard drive the next slot in Write Buffer can be filled
			/*while (true) {
				m_metaDataLock.lock();
				if (m_tickets == 0) {
					break;
				} else {
					m_blocked = true;
					m_metaDataLock.unlock();
				}
			}*/
			m_metaDataLock.lock();
			readPointer = m_bufferReadPointer;
			bytesInWriteBuffer = m_bytesInWriteBuffer;
			lengthByNode = m_lengthByNode;

			m_bufferReadPointer = m_bufferWritePointer;
			m_bytesInWriteBuffer = 0;
			m_lengthByNode = new int[Short.MAX_VALUE * 2];

			m_dataAvailable = false;
			m_flushingComplete = false;

			m_blocked = false;
			m_metaDataLock.unlock();


			if (bytesInWriteBuffer > 0) {
				// Write data to primary log
				try {
					writtenBytes = m_logHandler.getPrimaryLog().appendData(m_buffer,
							readPointer, bytesInWriteBuffer, lengthByNode);
				} catch (final IOException | InterruptedException e) {
					System.out.println("Error: Could not write to log");
				}
			}
			m_flushingComplete = true;

			return writtenBytes;
		}
	}
}
