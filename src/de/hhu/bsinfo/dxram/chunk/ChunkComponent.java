
package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class ChunkComponent extends AbstractDXRAMComponent {

	// dependent components
	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private LogComponent m_log;
	private LoggerComponent m_logger;

	/**
	 * Constructor
	 */
	public ChunkComponent() {
		super(DXRAMComponentOrder.Init.CHUNK, DXRAMComponentOrder.Shutdown.CHUNK);
	}

	/**
	 * Create the index chunk for the nameservice.
	 *
	 * @param p_size Size of the index chunk.
	 * @return Chunkid of the index chunk.
	 */
	public long createIndexChunk(final int p_size) {
		long chunkId = -1;

		m_memoryManager.lockManage();
		chunkId = m_memoryManager.createIndex(p_size);
		m_memoryManager.unlockManage();
		if (chunkId != -1) {
			m_backup.initBackupRange(chunkId, p_size);
		}

		return chunkId;
	}

	/**
	 * Internal chunk create for management data
	 *
	 * @param p_size Size of the chunk
	 * @return Chunkid of the created chunk.
	 */
	public long createChunk(final int p_size) {
		long chunkId = -1;

		m_memoryManager.lockManage();
		chunkId = m_memoryManager.create(p_size);
		m_memoryManager.unlockManage();
		if (chunkId != -1) {
			m_backup.initBackupRange(chunkId, p_size);
		}

		return chunkId;
	}

	/**
	 * Internal chunk put for management data.
	 *
	 * @param p_dataStructure Data structure to put
	 * @return True if successful, false otherwise
	 */
	public boolean putChunk(final DataStructure p_dataStructure) {

		MemoryErrorCodes err;
		m_memoryManager.lockAccess();
		err = m_memoryManager.put(p_dataStructure);
		m_memoryManager.unlockAccess();
		if (err != MemoryErrorCodes.SUCCESS) {
			return false;
		}

		if (m_backup.isActive()) {
			short[] backupPeers = m_backup.getBackupPeersForLocalChunks(p_dataStructure.getID());
			if (backupPeers != null) {
				for (short peer : backupPeers) {
					if (peer != m_boot.getNodeID() && peer != NodeID.INVALID_ID) {
						// #if LOGGER == TRACE
						m_logger.trace(getClass(),
								"Logging " + ChunkID.toHexString(p_dataStructure.getID()) + " to " + peer);
						// #endif /* LOGGER == TRACE */

						m_network.sendMessage(new LogMessage(peer, p_dataStructure));
					}
				}
			}
		}

		return true;
	}

	/**
	 * Put a recovered chunks into local memory.
	 *
	 * @param p_chunks Chunks to put.
	 */
	public void putRecoveredChunks(final Chunk[] p_chunks) {

		m_memoryManager.lockManage();
		for (Chunk chunk : p_chunks) {

			m_memoryManager.create(chunk.getID(), chunk.getDataSize());
			m_memoryManager.put(chunk);

			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "Stored recovered chunk " + chunk + " locally");
			// #endif /* LOGGER == TRACE */
		}
		m_memoryManager.unlockManage();
	}

	/**
	 * Puts migrated or recovered Chunks
	 *
	 * @param p_chunks the Chunks
	 * @return whether storing foreign chunks was successful or not
	 */
	public boolean putForeignChunks(final Chunk[] p_chunks) {
		byte rangeID;
		int logEntrySize;
		long size = 0;
		long cutChunkID = -1;
		short[] backupPeers = null;

		m_memoryManager.lockManage();
		for (Chunk chunk : p_chunks) {

			m_memoryManager.create(chunk.getID(), chunk.getDataSize());
			m_memoryManager.put(chunk);

			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "Stored migrated chunk " + chunk + " locally");
			// #endif /* LOGGER == TRACE */

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
			for (Chunk chunk : p_chunks) {

				if (chunk.getID() == cutChunkID) {
					// All following chunks are in the new migration backup range
					backupPeers = m_backup.getCurrentMigrationBackupPeers();
				}

				rangeID = m_backup.addMigratedChunk(chunk);

				if (backupPeers != null) {
					for (short backupPeer : backupPeers) {
						if (backupPeer != m_boot.getNodeID() && backupPeer != -1) {
							m_network.sendMessage(new LogMessage(backupPeer, rangeID, new Chunk[] {chunk}));
						}
					}
				}
			}
		}

		return true;
	}

	@Override
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
		m_backup = p_componentAccessor.getComponent(BackupComponent.class);
		m_memoryManager = p_componentAccessor.getComponent(MemoryManagerComponent.class);
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
		m_log = p_componentAccessor.getComponent(LogComponent.class);
		m_logger = p_componentAccessor.getComponent(LoggerComponent.class);
	}

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
