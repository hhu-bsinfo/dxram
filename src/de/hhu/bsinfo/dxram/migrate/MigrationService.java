package de.hhu.bsinfo.dxram.migrate;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.dxram.Core;
import de.uniduesseldorf.dxram.core.exceptions.MemoryException;
import de.uniduesseldorf.dxram.core.exceptions.ExceptionHandler.ExceptionSource;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.NodeRole;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.engine.DXRAMException;
import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.dxram.lock.LockComponent;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.log.LogMessages.LogMessage;
import de.hhu.bsinfo.dxram.log.LogMessages.RemoveMessage;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupException;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.migrate.messages.MigrationMessage;
import de.hhu.bsinfo.dxram.migrate.messages.MigrationMessages;
import de.hhu.bsinfo.dxram.migrate.messages.MigrationRequest;
import de.hhu.bsinfo.dxram.migrate.messages.MigrationResponse;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.ChunkID;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkException;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;
import de.hhu.bsinfo.utils.config.Configuration;

public class MigrationService extends DXRAMService implements MessageReceiver {
	
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
	// also: where to put this? where is it used?
//	/**
//	 * Puts migrated or recovered Chunks
//	 * @param p_chunks
//	 *            the Chunks
//	 * @throws LookupException
//	 *             if the backup range could not be initialized on superpeers
//	 * @throws NetworkException
//	 *             if the put Chunks could not be logged properly
//	 * @throws MemoryException
//	 *             if the Chunks could not be put properly
//	 */
//	public void putForeignDataStructures(final DataStructure[] p_dataStructures) {
//		int logEntrySize;
//		long size = 0;
//		long cutChunkID = -1;
//		short[] backupPeers = null;
//		Chunk chunk = null;
//
//		// multi put
//		/*-m_memoryManager.lockManage();
//		for (Chunk chunk : p_chunks) {
//			int bytesWritten;
//
//			m_memoryManager.create(chunk.getChunkID());
//			bytesWritten = m_memoryManager.put(chunk.getChunkID(), chunk.getData().array(), 0, chunk.getData().array().length);
//		}
//		m_memoryManager.unlockManage();*/
//
//		for (int i = 0; i < p_chunks.length; i++) {
//			chunk = p_chunks[i];
//
//			if (m_logActive) {
//				logEntrySize = chunk.getSize() + m_log.getAproxHeaderSize(ChunkID.getCreatorID(chunk.getChunkID()), ChunkID.getLocalID(chunk.getChunkID()),
//						chunk.getSize());
//				if (m_migrationsTree.fits(size + logEntrySize) && (m_migrationsTree.size() != 0 || size > 0)) {
//					// Chunk fits in current migration backup range
//					size += logEntrySize;
//				} else {
//					// Chunk does not fit -> initialize new migration backup range and remember cut
//					size = logEntrySize;
//					cutChunkID = chunk.getChunkID();
//
//					determineBackupPeers(-1);
//					m_migrationsTree.initNewBackupRange();
//
//					m_lookup.initRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(), new Locations(m_nodeID,
//							m_currentMigrationBackupRange.getBackupPeers(), null));
//					m_log.initBackupRange(((long) -1 << 48) + m_currentMigrationBackupRange.getRangeID(), m_currentMigrationBackupRange.getBackupPeers());
//				}
//			}
//		}
//
//		if (m_logActive) {
//			for (int i = 0; i < p_chunks.length; i++) {
//				chunk = p_chunks[i];
//
//				if (chunk.getChunkID() == cutChunkID) {
//					// All following chunks are in the new migration backup range
//					backupPeers = m_currentMigrationBackupRange.getBackupPeers();
//				}
//
//				m_migrationsTree.putObject(chunk.getChunkID(), (byte) m_currentMigrationBackupRange.getRangeID(), chunk.getSize());
//
//				if (backupPeers != null) {
//					for (int j = 0; j < backupPeers.length; j++) {
//						if (backupPeers[j] != getSystemData().getNodeID() && backupPeers[j] != -1) {
//							new LogMessage(backupPeers[j], (byte) m_currentMigrationBackupRange.getRangeID(),
//									new Chunk[] {chunk}).send(m_network);
//						}
//					}
//				}
//			}
//		}
//	}

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
			
			m_memoryManager.lockAccess();
			if (p_target != getSystemData().getNodeID() && m_memoryManager.exists(p_chunkID)) {
				int size;

				size = m_memoryManager.getSize(p_chunkID);
				chunk = new Chunk(p_chunkID, size);
				// TODO error handling
				m_memoryManager.get(chunk);
				m_memoryManager.unlockAccess();

				if (chunk != null) {
					LOGGER.trace("Send request to " + p_target);

					try {
						new MigrationMessage(p_target, new Chunk[] {chunk}).send(m_network);
					} catch (NetworkException e) {
						// TODO proper error handling
						e.printStackTrace();
					}

					// Update superpeers
					m_lookup.migrate(p_chunkID, p_target);
					// Remove all locks
					m_lock.unlockAll(p_chunkID);
					// Update local memory management
					m_memoryManager.remove(p_chunkID);
					if (m_logActive) {
						// TODO udpate logging: this should be done inside the log component
						// because we use a log message here (also duped code)
						backupPeers = m_backup.getBackupPeersForLocalChunks(p_chunkID);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
									try {
										new RemoveMessage(backupPeers[i], new long[] {p_chunkID}).send(m_network);
									} catch (NetworkException e) {
										// TODO proper error handling
										e.printStackTrace();
									}
								}
							}
						}
					}
					ret = true;
				}
			} else {
				m_memoryManager.unlockAccess();
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

		if (getSystemData().getNodeRole().equals(NodeRole.SUPERPEER)) {
			LOGGER.error("a superpeer must not store chunks");
		} else {
			if (p_startChunkID <= p_endChunkID) {
				chunkIDs = new long[(int) (p_endChunkID - p_startChunkID + 1)];
				m_migrationLock.lock();
				if (p_target != getSystemData().getNodeID()) {
					iter = p_startChunkID;
					while (true) {
						// Send chunks to p_target
						chunks = new Chunk[(int) (p_endChunkID - iter + 1)];
						counter = 0;
						size = 0;
						m_memoryManager.lockAccess();
						while (iter <= p_endChunkID) {
							if (m_memoryManager.exists(iter)) {
								int bytesRead;
								int sizeChunk;

								chunk = null;

								sizeChunk = m_memoryManager.getSize(iter);
								chunk = new Chunk(iter, sizeChunk);
								// TODO error handling
								m_memoryManager.get(chunk);

								chunks[counter] = chunk;
								chunkIDs[counter] = chunk.getID();
								size += chunk.sizeofObject();
							} else {
								System.out.println("Chunk with ChunkID " + iter + " could not be migrated!");
							}
							iter++;
						}
						m_memoryManager.unlockAccess();

						System.out.println("Sending " + counter + " Chunks (" + size + " Bytes) to " + p_target);
						try {
							new MigrationMessage(p_target, Arrays.copyOf(chunks, counter)).send(m_network);
						} catch (NetworkException e) {
							// TODO proper error handling
							e.printStackTrace();
						}

						if (iter > p_endChunkID) {
							break;
						}
					}

					// Update superpeers
					m_lookup.migrateRange(p_startChunkID, p_endChunkID, p_target);

					if (m_logActive) {
						// TODO udpate logging: this should be done inside the log component
						// because we use a log message here (also duped code)
						backupPeers = m_backup.getBackupPeersForLocalChunks(iter);
						if (backupPeers != null) {
							for (int i = 0; i < backupPeers.length; i++) {
								if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
									try {
										new RemoveMessage(backupPeers[i], chunkIDs).send(m_network);
									} catch (NetworkException e) {
										// TODO proper error handling
										e.printStackTrace();
									}
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
				boolean chunkExists = false;
				
				chunkID = ((long) getSystemData().getNodeID() << 48) + i;
				
				m_memoryManager.lockAccess();
				chunkExists = m_memoryManager.exists(chunkID);
				m_memoryManager.unlockAccess();
				
				if (chunkExists) {
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
		m_memoryManager.lockManage();
		if (p_target != getSystemData().getNodeID() && m_memoryManager.exists(p_chunkID) 
				&& m_memoryManager.dataWasMigrated(p_chunkID)) {
			
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
			try {
				new MigrationMessage(target, new DataStructure[] {chunk}).send(m_network);
			} catch (NetworkException e) {
				// TODO proper error handling
				e.printStackTrace();
			}

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
							try {
								new RemoveMessage(backupPeers[i], new long[] {p_chunkID}).send(m_network);
							} catch (NetworkException e) {
								// TODO proper error handling
								e.printStackTrace();
							}
						}
					}
				}
			}
		} else {
			m_memoryManager.unlockManage();
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
		if (p_target != getSystemData().getNodeID()) {
			int sizeChunk;

			m_memoryManager.lockAccess();
			sizeChunk = m_memoryManager.getSize(p_chunkID);
			chunk = new Chunk(p_chunkID, sizeChunk);
			// TODO error handling if failed
			m_memoryManager.get(chunk);
			m_memoryManager.unlockAccess();

			if (chunk != null) {
				LOGGER.trace("Send request to " + p_target);

				// This is not safe, but there is no other possibility unless
				// the number of network threads is increased
				System.out.println("** Migrating own chunk " + p_chunkID + " to " + p_target);
				try {
					new MigrationMessage(p_target, new Chunk[] {chunk}).send(m_network);
				} catch (NetworkException e) {
					// TODO error handling
					e.printStackTrace();
				}

				// Update superpeers
				m_lookup.migrateOwnChunk(p_chunkID, p_target);
				// Remove all locks
				m_lock.unlockAll(p_chunkID);
				// Update local memory management
				m_memoryManager.remove(p_chunkID);
				if (m_logActive) {
					// Update logging
					backupPeers = m_backup.getBackupPeersForLocalChunks(p_chunkID);
					if (backupPeers != null) {
						for (int i = 0; i < backupPeers.length; i++) {
							if (backupPeers[i] != getSystemData().getNodeID() && backupPeers[i] != -1) {
								try {
									new RemoveMessage(backupPeers[i], new long[] {p_chunkID}).send(m_network);
								} catch (NetworkException e) {
									// TODO error handling
									e.printStackTrace();
								}
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
				case MigrationMessages.SUBTYPE_MIGRATION_REQUEST:
					incomingMigrationRequest((MigrationRequest) p_message);
					break;
				case MigrationMessages.SUBTYPE_MIGRATION_MESSAGE:
					incomingMigrationMessage((MigrationMessage) p_message);
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
	private void incomingMigrationRequest(final MigrationRequest p_request) {
		putForeignDataStructures(p_request.getDataStructures());

		new MigrationResponse(p_request).send(m_network);
	}

	/**
	 * Handles an incoming DataMessage
	 * @param p_message
	 *            the DataMessage
	 */
	private void incomingMigrationMessage(final MigrationMessage p_message) {
		putForeignDataStructures(p_message.getDataStructures());
	}




}
