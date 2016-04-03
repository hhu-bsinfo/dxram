
package de.hhu.bsinfo.dxram.log;

import java.io.IOException;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.log.header.AbstractLogEntryHeader;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.InitResponse;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLog;
import de.hhu.bsinfo.dxram.log.storage.SecondaryLogBuffer;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;

/**
 * Component for remote logging of chunks.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class LogComponent extends DXRAMComponent {

	private BootComponent m_boot;
	private NetworkComponent m_network;
	private LoggerComponent m_logger;

	private LogService m_logService;
	private boolean m_useChecksum;
	private int m_logSegmentSize;
	private long m_secondaryLogSize;

	/**
	 * Creates the log component
	 * @param p_priorityInit
	 *            the initialization priority
	 * @param p_priorityShutdown
	 *            the shutdown priority
	 */
	public LogComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Returns the header size
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_localID
	 *            the LocalID
	 * @param p_size
	 *            the size of the Chunk
	 * @return the header size
	 */
	public short getAproxHeaderSize(final short p_nodeID, final long p_localID, final int p_size) {
		return AbstractLogEntryHeader.getAproxSecLogHeaderSize(m_boot.getNodeID() != p_nodeID, p_localID, p_size);
	}

	/**
	 * Initializes a new backup range
	 * @param p_firstChunkIDOrRangeID
	 *            the beginning of the range
	 * @param p_backupPeers
	 *            the backup peers
	 */
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
					request = new InitRequest(p_backupPeers[i], p_firstChunkIDOrRangeID, m_boot.getNodeID());
				}
				final NetworkErrorCodes err = m_network.sendSync(request);
				if (err != NetworkErrorCodes.SUCCESS) {
					i--;
					continue;
				}
				response = request.getResponse(InitResponse.class);

				if (!response.getStatus()) {
					i--;
				}
			}
		}
		m_logger.info(LogService.class, "Time to init range: " + (System.currentTimeMillis() - time));
	}

	/**
	 * Recovers all Chunks of given backup range
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_rangeID
	 *            the RangeID
	 * @return the recovered Chunks
	 */
	public Chunk[] recoverBackupRange(final short p_owner, final long p_chunkID, final byte p_rangeID) {
		Chunk[] chunks = null;
		SecondaryLogBuffer secondaryLogBuffer;

		try {
			m_logService.flushDataToPrimaryLog();

			secondaryLogBuffer = m_logService.getSecondaryLogBuffer(p_chunkID, p_owner, p_rangeID);
			if (secondaryLogBuffer != null) {
				secondaryLogBuffer.flushSecLogBuffer();
				chunks = m_logService.getSecondaryLog(p_chunkID, p_owner, p_rangeID).recoverAllLogEntries(true);
			}
		} catch (final IOException | InterruptedException e) {
			m_logger.error(LogService.class, "Backup range recovery failed: " + e);
		}

		return chunks;
	}

	/**
	 * Recovers all Chunks of given backup range
	 * @param p_fileName
	 *            the file name
	 * @param p_path
	 *            the path of the folder the file is in
	 * @return the recovered Chunks
	 */
	public Chunk[] recoverBackupRangeFromFile(final String p_fileName, final String p_path) {
		Chunk[] ret = null;

		try {
			ret = SecondaryLog.recoverBackupRangeFromFile(p_fileName, p_path, m_useChecksum, m_secondaryLogSize, m_logSegmentSize);
		} catch (final IOException | InterruptedException e) {
			m_logger.error(LogService.class, "Could not recover from file " + p_path + ": " + e);
		}

		return ret;
	}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {

		m_boot = getDependentComponent(BootComponent.class);
		m_network = getDependentComponent(NetworkComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);

		return true;
	}

	/**
	 * Sets attributes from log service
	 * @param p_logService
	 *            the log service
	 * @param p_backupDirectory
	 *            the backup directory
	 * @param p_useChecksum
	 *            whether checksums are used or not
	 * @param p_secondaryLogSize
	 *            the secondary log size
	 * @param p_logSegmentSize
	 *            the segment size
	 */
	protected void setAttributes(final LogService p_logService, final String p_backupDirectory,
			final boolean p_useChecksum, final long p_secondaryLogSize, final int p_logSegmentSize) {
		m_logService = p_logService;
		m_useChecksum = p_useChecksum;
		m_secondaryLogSize = p_secondaryLogSize;
		m_logSegmentSize = p_logSegmentSize;
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {}

	@Override
	protected boolean shutdownComponent() {
		return false;
	}

}
