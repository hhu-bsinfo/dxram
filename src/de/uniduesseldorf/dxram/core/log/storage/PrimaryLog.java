
package de.uniduesseldorf.dxram.core.log.storage;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;

import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.dxram.nodeconfig.NodeID;
import de.uniduesseldorf.dxram.core.log.LogHandler;

import de.uniduesseldorf.utils.config.Configuration.ConfigurationConstants;

/**
 * This class implements the primary log. Furthermore this class manages all
 * secondary logs
 * @author Kevin Beineke 13.06.2014
 */
public final class PrimaryLog extends AbstractLog {

	// Constants
	private static final String BACKUP_DIRECTORY = Core.getConfiguration().getStringValue(DXRAMConfigurationConstants.LOG_DIRECTORY);
	private static final String PRIMLOG_SUFFIX_FILENAME = "prim.log";
	private static final byte[] PRIMLOG_HEADER = "DXRAMPrimLogv1".getBytes(Charset.forName("UTF-8"));
	private static final long PRIMLOG_SIZE = Core.getConfiguration().getLongValue(DXRAMConfigurationConstants.PRIMARY_LOG_SIZE);
	private static final int PRIMLOG_MIN_SIZE = 65535 * FLASHPAGE_SIZE;

	// Attributes
	private LogHandler m_logHandler;

	private long m_writePos;
	private long m_numberOfBytes;

	// Constructors
	/**
	 * Creates an instance of PrimaryLog with user specific configuration
	 * @param p_logHandler
	 *            the log handler
	 * @throws IOException
	 *             if primary log could not be created
	 * @throws InterruptedException
	 *             if the caller was interrupted
	 */
	public PrimaryLog(final LogHandler p_logHandler) throws IOException, InterruptedException {
		super(new File(BACKUP_DIRECTORY + "N" + NodeID.getLocalNodeID() + "_" + PRIMLOG_SUFFIX_FILENAME), PRIMLOG_SIZE,
				PRIMLOG_HEADER.length);
		m_writePos = 0;
		m_numberOfBytes = 0;

		m_logHandler = p_logHandler;

		if (PRIMLOG_SIZE < PRIMLOG_MIN_SIZE) {
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
		if (PRIMLOG_SIZE - m_numberOfBytes < p_length) {
			// Not enough free space in primary log -> flush to secondary logs and reset primary log
			m_logHandler.flushDataToSecondaryLogs();
			m_numberOfBytes = 0;
		}

		m_writePos = appendToPrimaryLog(p_data, p_offset, p_length, m_writePos);
		m_numberOfBytes += p_length;

		return p_length;
	}

}
