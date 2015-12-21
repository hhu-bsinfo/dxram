package de.uniduesseldorf.dxram.core.migrate;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.backup.BackupComponent;
import de.uniduesseldorf.dxram.core.chunk.ChunkService;
import de.uniduesseldorf.dxram.core.chunk.messages.ChunkMessages;
import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.engine.DXRAMException;
import de.uniduesseldorf.dxram.core.engine.DXRAMService;
import de.uniduesseldorf.dxram.core.engine.nodeconfig.NodeRole;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.lock.LockComponent;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;
import de.uniduesseldorf.dxram.core.log.LogComponent;
import de.uniduesseldorf.dxram.core.log.LogMessages.LogMessage;
import de.uniduesseldorf.dxram.core.log.LogMessages.RemoveMessage;
import de.uniduesseldorf.dxram.core.lookup.LookupComponent;
import de.uniduesseldorf.dxram.core.lookup.LookupException;
import de.uniduesseldorf.dxram.core.mem.Chunk;
import de.uniduesseldorf.dxram.core.mem.DataStructure;
import de.uniduesseldorf.dxram.core.mem.storage.MemoryManagerComponent;
import de.uniduesseldorf.dxram.core.migrate.messages.MigrationMessage;
import de.uniduesseldorf.dxram.core.migrate.messages.MigrationMessages;
import de.uniduesseldorf.dxram.core.migrate.messages.MigrationRequest;
import de.uniduesseldorf.dxram.core.migrate.messages.MigrationResponse;
import de.uniduesseldorf.dxram.core.net.NetworkComponent;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.data.Chunk;
import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.menet.NodeID;
import de.uniduesseldorf.utils.config.Configuration;

public class MigrationService extends DXRAMService {
	
	private static final String SERVICE_NAME = "Migration";
	
	private final Logger LOGGER = Logger.getLogger(MigrationService.class);
	
	private NetworkComponent m_network = null;
	private MemoryManagerComponent m_memoryManager = null;
	private LookupComponent m_lookup = null;
	private LockComponent m_lock = null;
	private BackupComponent m_backup = null;
	
	private ReentrantLock m_migrationLock = null;
	
	private boolean m_logActive = false;
	
	public MigrationService() {
		super(SERVICE_NAME);
	}
	
	@Override
	protected boolean startService(Configuration p_configuration) {
		m_network = getComponent(NetworkComponent.COMPONENT_IDENTIFIER);
		m_memoryManager = getComponent(MemoryManagerComponent.COMPONENT_IDENTIFIER);
		m_lookup = getComponent(LookupComponent.COMPONENT_IDENTIFIER);
		m_lock = getComponent(LockComponent.COMPONENT_IDENTIFIER);
		m_backup = getComponent(BackupComponent.COMPONENT_IDENTIFIER);
		
		// TODO get config values
		
		m_migrationLock = new ReentrantLock(false);
		
		m_network.registerMessageType(MigrationMessages.TYPE, MigrationMessages.SUBTYPE_MIGRATION_REQUEST, MigrationRequest.class);
		m_network.registerMessageType(MigrationMessages.TYPE, MigrationMessages.SUBTYPE_MIGRATION_RESPONSE, MigrationResponse.class);
		return false;
	}

	@Override
	protected boolean shutdownService() {
		// TODO Auto-generated method stub
		return false;
	}
	
	// ---------------------------------------------------------------------------------------------
	
	// TODO have this split into migrated and recovered chunks?
	/**
	 * Puts migrated or recovered Chunks
	 * @param p_chunks
	 *            the Chunks
	 * @throws LookupException
	 *             if the backup range could not be initialized on superpeers
	 * @throws NetworkException
	 *             if the put Chunks could not be logged properly
	 * @throws MemoryException
	 *             if the Chunks could not be put properly
	 */
	public void putForeignChunks(final Chunk[] p_chunks) {
		int logEntrySize;
		long size = 0;
		long cutChunkID = -1;
		short[] backupPeers = null;
		Chunk chunk = null;

		// multi put
		/*-m_memoryManager.lockManage();
		for (Chunk chunk : p_chunks) {
			int bytesWritten;

			m_memoryManager.create(chunk.getChunkID());
			bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
		}
		m_memoryManager.unlockManage();*/

		for (int i = 0; i < p_chunks.length; i++) {
			chunk = p_chunks[i];

			if (m_logActive) {
				logEntrySize = chunk.getSize() + m_log.getAproxHeaderSize(ChunkID.getCreatorID(chunk.getChunkID()), ChunkID.getLocalID(chunk.getChunkID()),
						chunk.getSize());
				if (m_migrationsTree.fits(size + logEntrySize) && (m_migrationsTree.size() != 0 || size > 0)) {
					// Chunk fits in current migration backup range
					size += logEntrySize;
				} else {
					// Chunk does not fit -> initialize new migration backup range and remember cut
					size = logEntrySize;
					cutChunkID = chunk.getChunkID();

					determineBackupPeers(-1);
					m_migrationsTree.initNewBackupRange();

					m_lookup.initRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(), new Locations(m_nodeID,
							m_currentMigrationBackupRange.getBackupPeers(), null));
					m_log.initBackupRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(), m_currentMigrationBackupRange.getBackupPeers());
				}
			}
		}

		if (LOG_ACTIVE) {
			for (int i = 0; i < p_chunks.length; i++) {
				chunk = p_chunks[i];

				if (chunk.getChunkID() == cutChunkID) {
					// All following chunks are in the new migration backup range
					backupPeers = m_currentMigrationBackupRange.getBackupPeers();
				}

				m_migrationsTree.putObject(chunk.getChunkID(), (byte) m_currentMigrationBackupRange.getRangeID(), chunk.getSize());

				if (backupPeers != null) {
					for (int j = 0; j < backupPeers.length; j++) {
						if (backupPeers[j] != m_nodeID && backupPeers[j] != -1) {
							new LogMessage(backupPeers[j], (byte) m_currentMigrationBackupRange.getRangeID(),
									new Chunk[] {chunk}).send(m_network);
						}
					}
				}
			}
		}
	}

	public boolean migrate(final long p_chunkID, final short p_target) {
		short[] backupPeers;
		Chunk chunk;
		boolean ret = false;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			m_migrationLock.lock();
			if (p_target != m_nodeID && m_memoryManager.isResponsible(p_chunkID)) {
				int size;
				int bytesRead;

				chunk = null;

				m_memoryManager.lockAccess();
				size = m_memoryManager.getSize(p_chunkID);
				chunk = new Chunk(p_chunkID, size);
				bytesRead = m_memoryManager.get(p_chunkID, chunk.getData().array(), 0, size);
				m_memoryManager.unlockAccess();

				if (chunk != null) {
					LOGGER.trace("Send request to " + p_target);

					new DataMessage(p_target, new Chunk[] {chunk}).send(m_network);

					// Update superpeers
					m_lookup.migrate(p_chunkID, p_target);
					// Remove all locks
					m_lock.unlockAll(p_chunkID);
					// Update local memory management
					m_memoryManager.remove(p_chunkID);
					if (LOG_ACTIVE) {
						// Update logging
						backupPeers = getBackupPeersForLocalChunks(p_chunkID);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], new long[] {p_chunkID}).send(m_network);
								}
							}
						}
					}
					ret = true;
				}
			} else {
				System.out.println("Chunk with ChunkID " + p_chunkID + " could not be migrated!");
				ret = false;
			}
			m_migrationLock.unlock();
		}
		return ret;
	}

	public boolean migrateRange(final long p_startChunkID, final long p_endChunkID, final short p_target) {
		long[] chunkIDs = null;
		short[] backupPeers;
		int counter = 0;
		long iter;
		long size;
		Chunk chunk;
		Chunk[] chunks;
		boolean ret = false;

		ChunkID.check(p_startChunkID);
		ChunkID.check(p_endChunkID);
		NodeID.check(p_target);

		// TODO: Handle range properly

		if (NodeID.getRole().equals(Role.SUPERPEER)) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			if (p_startChunkID <= p_endChunkID) {
				chunkIDs = new long[(int) (p_endChunkID - p_startChunkID + 1)];
				m_migrationLock.lock();
				if (p_target != m_nodeID) {
					iter = p_startChunkID;
					while (true) {
						// Send chunks to p_target
						chunks = new Chunk[(int) (p_endChunkID - iter + 1)];
						counter = 0;
						size = 0;
						m_memoryManager.lockAccess();
						while (iter <= p_endChunkID) {
							if (m_memoryManager.isResponsible(iter)) {
								int bytesRead;
								int sizeChunk;

								chunk = null;

								sizeChunk = m_memoryManager.getSize(iter);
								chunk = new Chunk(iter, sizeChunk);
								bytesRead = m_memoryManager.get(iter, chunk.getData().array(), 0, sizeChunk);

								chunks[counter] = chunk;
								chunkIDs[counter] = chunk.getChunkID();
								size += chunk.getSize();
							} else {
								System.out.println("Chunk with ChunkID " + iter + " could not be migrated!");
							}
							iter++;
						}
						m_memoryManager.unlockAccess();

						System.out.println("Sending " + counter + " Chunks (" + size + " Bytes) to " + p_target);
						new DataMessage(p_target, Arrays.copyOf(chunks, counter)).send(m_network);

						if (iter > p_endChunkID) {
							break;
						}
					}

					// Update superpeers
					m_lookup.migrateRange(p_startChunkID, p_endChunkID, p_target);

					if (LOG_ACTIVE) {
						// Update logging
						backupPeers = getBackupPeersForLocalChunks(iter);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
									new RemoveMessage(backupPeers[i], chunkIDs).send(m_network);
								}
							}
						}
					}

					iter = p_startChunkID;
					while (iter <= p_endChunkID) {
						// Remove all locks
						m_lock.unlockAll(iter);
						// Update local memory management
						m_memoryManager.remove(iter);
						iter++;
					}
					ret = true;
				} else {
					System.out.println("Chunks could not be migrated because end of range is before start of range!");
					ret = false;
				}
			} else {
				System.out.println("Chunks could not be migrated!");
				ret = false;
			}
			m_migrationLock.unlock();
			System.out.println("All chunks migrated!");
		}
		return ret;
	}
	
	public void migrateAll(final short p_target) throws DXRAMException {
		long localID;
		long chunkID;
		Iterator<Long> iter;

		// TODO
		localID = -1;
		// localID = m_memoryManager.getCurrentLocalID();

		// Migrate own chunks to p_target
		if (1 != localID) {
			for (int i = 1; i <= localID; i++) {
				chunkID = ((long) m_nodeID << 48) + i;
				if (m_memoryManager.isResponsible(chunkID)) {
					migrateOwnChunk(chunkID, p_target);
				}
			}
		}

		// TODO
		iter = null;
		// iter = m_memoryManager.getCIDOfAllMigratedChunks().iterator();
		while (iter.hasNext()) {
			chunkID = iter.next();
			migrateNotCreatedChunk(chunkID, p_target);
		}
	}
	
	// ---------------------------------------------------------------------------------------------

	/**
	 * Migrates a Chunk that was not created on this node; is called during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be migrated
	 */
	private void migrateNotCreatedChunk(final long p_chunkID, final short p_target) {
		DataStructure chunk;
		short creator;
		short target;
		short[] backupPeers;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);
		creator = ChunkID.getCreatorID(p_chunkID);

		m_migrationLock.lock();
		if (p_target != getSystemData().getNodeID() && m_memoryManager.isResponsible(p_chunkID) 
				&& m_memoryManager.wasMigrated(p_chunkID)) {
			
			m_memoryManager.lockManage();
			chunk = new Chunk(p_chunkID, m_memoryManager.getSize(p_chunkID));
			// TODO error handling
			m_memoryManager.get(chunk);
			m_memoryManager.remove(p_chunkID);
			m_memoryManager.lockManage();
			
			LOGGER.trace("Send request to " + p_target);

			if (m_lookup.creatorAvailable(creator)) {
				// Migrate chunk back to owner
				System.out.println("** Migrating " + p_chunkID + " back to " + creator);
				target = creator;
			} else {
				// Migrate chunk to p_target, if owner is not available anymore
				System.out.println("** Migrating " + p_chunkID + " to " + p_target);
				target = p_target;
			}

			// This is not safe, but there is no other possibility unless
			// the number of network threads is increased
			new MigrationMessage(target, new DataStructure[] {chunk}).send(m_network);

			// Update superpeers
			m_lookup.migrateNotCreatedChunk(p_chunkID, target);
			// Remove all locks
			m_lock.unlockAll(p_chunkID);
			if (m_logActive) {
				// Update logging
				backupPeers = m_backup.getBackupPeersForLocalChunks(p_chunkID);
				if (backupPeers != null) {
					for (int i = 0; i < backupPeers.length; i++) {
						if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
							new RemoveMessage(backupPeers[i], new long[] {p_chunkID}).send(m_network);
						}
					}
				}
			}
		}
		m_migrationLock.unlock();
	}

	/**
	 * Migrates a Chunk that was created on this node; is called during promotion
	 * @param p_chunkID
	 *            the ID
	 * @param p_target
	 *            the Node where to migrate the Chunk
	 * @throws DXRAMException
	 *             if the Chunk could not be migrated
	 */
	private void migrateOwnChunk(final long p_chunkID, final short p_target) {
		short[] backupPeers;
		Chunk chunk;

		ChunkID.check(p_chunkID);
		NodeID.check(p_target);

		m_migrationLock.lock();
		if (p_target != m_nodeID) {
			int sizeChunk;
			int bytesRead;

			chunk = null;

			m_memoryManager.lockAccess();
			sizeChunk = m_memoryManager.getSize(p_chunkID);
			chunk = new Chunk(p_chunkID, sizeChunk);
			bytesRead = m_memoryManager.get(p_chunkID, chunk.getData().array(), 0, sizeChunk);
			m_memoryManager.unlockAccess();

			if (chunk != null) {
				LOGGER.trace("Send request to " + p_target);

				// This is not safe, but there is no other possibility unless
				// the number of network threads is increased
				System.out.println("** Migrating own chunk " + p_chunkID + " to " + p_target);
				new DataMessage(p_target, new Chunk[] {chunk}).send(m_network);

				// Update superpeers
				m_lookup.migrateOwnChunk(p_chunkID, p_target);
				// Remove all locks
				m_lock.unlockAll(p_chunkID);
				// Update local memory management
				m_memoryManager.remove(p_chunkID);
				if (LOG_ACTIVE) {
					// Update logging
					backupPeers = getBackupPeersForLocalChunks(p_chunkID);
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != m_nodeID && backupPeers[i] != -1) {
								new RemoveMessage(backupPeers[i], new long[] {p_chunkID}).send(m_network);
							}
						}
					}
				}
			}
		}
		m_migrationLock.unlock();
	}
	
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		LOGGER.trace("Entering incomingMessage with: p_message=" + p_message);

		if (p_message != null) {
			if (p_message.getType() == ChunkMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case ChunkMessages.SUBTYPE_DATA_REQUEST:
					incomingDataRequest((DataRequest) p_message);
					break;
				case ChunkMessages.SUBTYPE_DATA_MESSAGE:
					incomingDataMessage((DataMessage) p_message);
					break;
				default:
					break;
				}
			}
		}

		LOGGER.trace("Exiting incomingMessage");
	}
	
	/**
	 * Handles an incoming DataRequest
	 * @param p_request
	 *            the DataRequest
	 */
	private void incomingDataRequest(final DataRequest p_request) {
		try {
			putForeignChunks(p_request.getChunks());

			new DataResponse(p_request).send(m_network);
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle request", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_request);
		}
	}

	/**
	 * Handles an incoming DataMessage
	 * @param p_message
	 *            the DataMessage
	 */
	private void incomingDataMessage(final DataMessage p_message) {
		try {
			putForeignChunks(p_message.getChunks());
		} catch (final DXRAMException e) {
			LOGGER.error("ERR::Could not handle message", e);

			Core.handleException(e, ExceptionSource.DATA_INTERFACE, p_message);
		}
	}




}
