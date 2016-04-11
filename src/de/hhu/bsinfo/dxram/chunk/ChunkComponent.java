
package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Component for chunk handling.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class ChunkComponent extends AbstractDXRAMComponent {

	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private LogComponent m_log;
	private LoggerComponent m_logger;

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public ChunkComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	/**
	 * Put a recovered chunks into local memory.
	 * @param p_chunks
	 *            Chunks to put.
	 */
	public void putRecoveredChunks(final Chunk[] p_chunks) {

		if (!m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			putForeignChunks(p_chunks);
		}
	}

	/**
	 * Puts migrated or recovered Chunks
	 * @param p_chunks
	 *            the Chunks
	 * @return whether storing foreign chunks was successful or not
	 */
	public boolean putForeignChunks(final Chunk[] p_chunks) {
		boolean ret = true;
		byte rangeID;
		int logEntrySize;
		long size = 0;
		long cutChunkID = -1;
		short[] backupPeers = null;
		Chunk chunk = null;

		m_memoryManager.lockManage();
		for (int i = 0; i < p_chunks.length; i++) {
			chunk = p_chunks[i];

			m_memoryManager.create(chunk.getID(), chunk.getDataSize());
			m_memoryManager.put(chunk);

			m_logger.trace(getClass(), "Stored migrated chunk " + chunk + " locally");

			if (m_backup.isActive()) {
				logEntrySize = chunk.getDataSize() + m_log.getAproxHeaderSize(ChunkID.getCreatorID(chunk.getID()),
						ChunkID.getLocalID(chunk.getID()),
						chunk.getDataSize());
				if (m_backup.fitsInCurrentMigrationBackupRange(size, logEntrySize)) {
					// Chunk fits in current migration backup range
					size += logEntrySize;
				} else {
					// Chunk does not fit -> initialize new migration backup range and remember cut
					size = logEntrySize;
					cutChunkID = chunk.getID();

					m_backup.initNewMigrationBackupRange();
				}
			}
		}
		m_memoryManager.unlockManage();

		if (m_backup.isActive()) {
			for (int i = 0; i < p_chunks.length; i++) {
				chunk = p_chunks[i];

				if (chunk.getID() == cutChunkID) {
					// All following chunks are in the new migration backup range
					backupPeers = m_backup.getCurrentMigrationBackupPeers();
				}

				rangeID = m_backup.addMigratedChunk(chunk);

				if (backupPeers != null) {
					for (int j = 0; j < backupPeers.length; j++) {
						if (backupPeers[j] != m_boot.getNodeID() && backupPeers[j] != -1) {
							m_network.sendMessage(new LogMessage(backupPeers[j], rangeID, new Chunk[] {chunk}));
						}
					}
				}
			}
		}

		return ret;
	}

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {}

	@Override
	protected boolean initComponent(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		m_boot = getDependentComponent(AbstractBootComponent.class);
		m_backup = getDependentComponent(BackupComponent.class);
		m_memoryManager = getDependentComponent(MemoryManagerComponent.class);
		m_network = getDependentComponent(NetworkComponent.class);
		m_log = getDependentComponent(LogComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
