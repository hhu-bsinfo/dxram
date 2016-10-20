
package de.hhu.bsinfo.dxram.log.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import de.hhu.bsinfo.dxram.log.LogService;

/**
 * This class implements the primary log. Furthermore this class manages all
 * secondary logs
 * @author Kevin Beineke 13.06.2014
 */
public final class PrimaryLog extends AbstractLog {

	// Constants
	private static final String PRIMLOG_SUFFIX_FILENAME = "prim.log";
	private static final byte[] PRIMLOG_HEADER = "DXRAMPrimLogv1".getBytes(Charset.forName("UTF-8"));

	// Attributes
	private LogService m_logService;
	private long m_primaryLogSize;

	private long m_writePos;
	private long m_numberOfBytes;

	// Constructors
	/**
	 * Creates an instance of PrimaryLog with user specific configuration
	 * @param p_logService
	 *            the log service
	 * @param p_backupDirectory
	 *            the backup directory
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_primaryLogSize
	 *            the size of a primary log
	 * @param p_flashPageSize
	 *            the size of flash page
	 * @throws IOException
	 *             if primary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public PrimaryLog(final LogService p_logService, final String p_backupDirectory, final short p_nodeID, final long p_primaryLogSize,
			final int p_flashPageSize) throws IOException, InterruptedException {
		super(new File(p_backupDirectory + "N" + p_nodeID + "_" + PRIMLOG_SUFFIX_FILENAME), p_primaryLogSize,
				PRIMLOG_HEADER.length);
		m_primaryLogSize = p_primaryLogSize;

		m_writePos = 0;
		m_numberOfBytes = 0;

		m_logService = p_logService;

		if (m_primaryLogSize < p_flashPageSize) {
			throw new IllegalArgumentException("Error: Primary log too small");
		}

		createLogAndWriteHeader(PRIMLOG_HEADER);
	}

	// Methods
	@Override
	public long getOccupiedSpace() {
		return m_numberOfBytes;
	}

	@Override
	public int appendData(final byte[] p_data, final int p_offset, final int p_length) throws IOException,
	InterruptedException {
		if (m_primaryLogSize - m_numberOfBytes < p_length) {
			// Not enough free space in primary log -> flush to secondary logs and reset primary log
			m_logService.flushDataToSecondaryLogs();
			m_numberOfBytes = 0;
		}

		m_writePos = appendToPrimaryLog(p_data, p_offset, p_length, m_writePos);
		m_numberOfBytes += p_length;

		return p_length;
	}

}
