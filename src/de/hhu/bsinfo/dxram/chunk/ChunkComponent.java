
package de.hhu.bsinfo.dxram.chunk;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.messages.InitRequest;
import de.hhu.bsinfo.dxram.log.messages.LogMessage;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent.MemoryErrorCodes;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.ethnet.NodeID;

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
	 * Create the index chunk for the nameservice.
	 * @param p_size
	 *            Size of the index chunk.
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
	 * @param p_size
	 *            Size of the chunk
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
	 * @param p_dataStructure
	 *            Data structure to put
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
	 * Replicates all local Chunks of given range to a specific backup peer
	 * @param p_backupPeer
	 *            the new backup peer
	 * @param p_firstChunkID
	 *            the first ChunkID
	 * @param p_lastChunkID
	 *            the last ChunkID
	 */
	public void replicateBackupRange(final short p_backupPeer, final long p_firstChunkID, final long p_lastChunkID) {
		int counter = 0;
		Chunk currentChunk;
		Chunk[] chunks;

		// Initialize backup range on backup peer
		InitRequest request = new InitRequest(p_backupPeer, p_firstChunkID, m_boot.getNodeID());
		NetworkErrorCodes err = m_network.sendMessage(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER == ERROR
			m_logger.error(getClass(), "Replicating backup range " + p_firstChunkID
					+ " to " + p_backupPeer + " failed. Could not initialize backup range.");
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
		m_network.sendMessage(new LogMessage(p_backupPeer, chunks));
	}

	/**
	 * Replicates all local Chunks to a specific backup peer
	 * @param p_backupPeer
	 *            the new backup peer
	 * @param p_chunkIDs
	 *            the ChunkIDs of the Chunks to replicate
	 * @param p_rangeID
	 *            the RangeID
	 */
	public void replicateBackupRange(final short p_backupPeer, final long[] p_chunkIDs, final byte p_rangeID) {
		int counter = 0;
		Chunk currentChunk;
		Chunk[] chunks;

		// Initialize backup range on backup peer
		InitRequest request = new InitRequest(p_backupPeer, p_rangeID, m_boot.getNodeID());
		NetworkErrorCodes err = m_network.sendMessage(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER == ERROR
			m_logger.error(getClass(), "Replicating backup range " + p_rangeID
					+ " to " + p_backupPeer + " failed. Could not initialize backup range.");
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
		m_network.sendMessage(new LogMessage(p_backupPeer, chunks));
	}

	/**
	 * Put a recovered chunks into local memory.
	 * @param p_chunks
	 *            Chunks to put.
	 */
	public void putRecoveredChunks(final Chunk[] p_chunks) {
		Chunk chunk = null;

		m_memoryManager.lockManage();
		for (int i = 0; i < p_chunks.length; i++) {
			chunk = p_chunks[i];

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
			for (int i = 0; i < p_chunks.length; i++) {
				chunk = p_chunks[i];

				if (chunk.getID() == cutChunkID) {
					// All following chunks are in the new migration backup range
					backupPeers = m_backup.getCopyOfCurrentMigrationBackupPeers();
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
