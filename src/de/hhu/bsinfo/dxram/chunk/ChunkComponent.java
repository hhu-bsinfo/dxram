package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

public abstract class ChunkComponent extends DXRAMComponent {
	
	private BootComponent m_boot;
	private BackupComponent m_backup;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private LogComponent m_log;
	
	public ChunkComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}
	
	public void putRecoveredChunks(final Chunk[] p_chunks) {

		if (!m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
			putForeignChunks(p_chunks);
		}
	}
	
	@Override
	protected boolean initComponent(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings, Settings p_settings) {

		m_boot = getDependentComponent(BootComponent.class);
		m_backup = getDependentComponent(BackupComponent.class);
		m_memoryManager = getDependentComponent(MemoryManagerComponent.class);
		m_network = getDependentComponent(NetworkComponent.class);
		m_log = getDependentComponent(LogComponent.class);
		
		return false;
	}

	/**
	 * Puts migrated or recovered Chunks
	 * @param p_chunks
	 *            the Chunks
	 */
	public void putForeignChunks(final Chunk[] p_chunks) {
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

			if (m_backup.isActive()) {
				logEntrySize = chunk.getDataSize() + m_log.getAproxHeaderSize(ChunkID.getCreatorID(chunk.getID()), ChunkID.getLocalID(chunk.getID()),
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
	}
	
}
