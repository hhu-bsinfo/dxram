
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
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NodeID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Component for chunk handling.
 *
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class ChunkComponent extends AbstractDXRAMComponent {

	private static final Logger LOGGER = LogManager.getFormatterLogger(ChunkComponent.class.getSimpleName());

	// dependent components
	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private LogComponent m_log;

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
		if (chunkId != -1) {
			m_backup.initBackupRange(chunkId, p_size);
		}
		m_memoryManager.unlockManage();

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
		if (chunkId != -1) {
			m_backup.initBackupRange(chunkId, p_size);
		}
		m_memoryManager.unlockManage();

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
			short[] backupPeers = m_backup.getCopyOfBackupPeersForLocalChunks(p_dataStructure.getID());

			if (backupPeers != null) {
				for (short peer : backupPeers) {
					if (peer != m_boot.getNodeID() && peer != NodeID.INVALID_ID) {
						// #if LOGGER == TRACE
						LOGGER.trace("Logging %s to %s",
								ChunkID.toHexString(p_dataStructure.getID()), NodeID.toHexString(peer));
						// #endif /* LOGGER == TRACE */

						try {
							m_network.sendMessage(new LogMessage(peer, p_dataStructure));
						} catch (final NetworkException e) {

						}
					}
				}
			}
		}

		return true;
	}

	/**
	 * Replicates all local Chunks of given range to a specific backup peer
	 *
	 * @param p_backupPeer   the new backup peer
	 * @param p_firstChunkID the first ChunkID
	 * @param p_lastChunkID  the last ChunkID
	 */
	public void replicateBackupRange(final short p_backupPeer, final long p_firstChunkID, final long p_lastChunkID) {
		int counter = 0;
		Chunk currentChunk;
		Chunk[] chunks;

		// Initialize backup range on backup peer
		InitRequest request = new InitRequest(p_backupPeer, p_firstChunkID, m_boot.getNodeID());
		try {
			m_network.sendMessage(request);
		} catch (final NetworkException e) {
			// #if LOGGER == ERROR
			LOGGER.error("Replicating backup range 0x%X to 0x%X failed. Could not initialize backup range",
					p_firstChunkID, p_backupPeer);
			// #endif /* LOGGER == ERROR */
			return;
		}

		// Gather all chunks of backup range
		chunks = new Chunk[(int) (p_lastChunkID - p_firstChunkID + 1)];
		for (long chunkID = p_firstChunkID; chunkID <= p_lastChunkID; chunkID++) {
			currentChunk = new Chunk(chunkID);

			m_memoryManager.lockAccess();
			m_memoryManager.get(currentChunk);
			m_memoryManager.unlockAccess();

			chunks[counter++] = currentChunk;
		}

		// Send all chunks to backup peer
		try {
			m_network.sendMessage(new LogMessage(p_backupPeer, chunks));
		} catch (final NetworkException e) {

		}
	}

	/**
	 * Replicates all local Chunks to a specific backup peer
	 *
	 * @param p_backupPeer the new backup peer
	 * @param p_chunkIDs   the ChunkIDs of the Chunks to replicate
	 * @param p_rangeID    the RangeID
	 */
	public void replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDs, final byte p_rangeID) {
		int counter = 0;
		Chunk currentChunk;
		Chunk[] chunks;

		// Initialize backup range on backup peer
		InitRequest request = new InitRequest(p_backupPeer, p_rangeID, m_boot.getNodeID());

		try {
			m_network.sendMessage(request);
		} catch (final NetworkException e) {
			// #if LOGGER == ERROR
			LOGGER.error("Replicating backup range 0x%X to 0x%X failed. Could not initialize backup range",
					p_rangeID, p_backupPeer);
			// #endif /* LOGGER == ERROR */
			return;
		}

		// Gather all chunks of backup range
		chunks = new Chunk[p_chunkIDs.length];
		for (long chunkID : p_chunkIDs) {
			currentChunk = new Chunk(chunkID);

			m_memoryManager.lockAccess();
			m_memoryManager.get(currentChunk);
			m_memoryManager.unlockAccess();

			chunks[counter++] = currentChunk;
		}

		// Send all chunks to backup peer
		try {
			m_network.sendMessage(new LogMessage(p_backupPeer, chunks));
		} catch (final NetworkException e) {

		}
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
			LOGGER.trace("Stored recovered chunk %s locally", ChunkID.toHexString(chunk.getID()));
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
			LOGGER.trace("Stored migrated chunk %s locally", ChunkID.toHexString(chunk.getID()));
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
					backupPeers = m_backup.getCopyOfCurrentMigrationBackupPeers();
				}

				rangeID = m_backup.addMigratedChunk(chunk);

				if (backupPeers != null) {
					for (short backupPeer : backupPeers) {
						if (backupPeer != m_boot.getNodeID() && backupPeer != -1) {
							try {
								m_network.sendMessage(new LogMessage(backupPeer, rangeID, new Chunk[] {chunk}));
							} catch (NetworkException e) {

							}
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
