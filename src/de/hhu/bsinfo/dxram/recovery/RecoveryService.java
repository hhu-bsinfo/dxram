
package de.hhu.bsinfo.dxram.recovery;

import java.io.File;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.log.LogComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeRequest;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverBackupRangeResponse;
import de.hhu.bsinfo.dxram.recovery.messages.RecoverMessage;
import de.hhu.bsinfo.dxram.recovery.messages.RecoveryMessages;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;

/**
 * This service provides all recovery functionality.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 31.03.16
 */
public class RecoveryService extends AbstractDXRAMService implements MessageReceiver {
	// Attributes
	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private ChunkComponent m_chunk;
	private LogComponent m_log;
	private LoggerComponent m_logger;
	private LookupComponent m_lookup;
	private NetworkComponent m_network;

	private String m_backupDirectory;

	/**
	 * Constructor
	 */
	public RecoveryService() {
		super("recovery");
	}

	/**
	 * Recovers all Chunks of given node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @param p_dest
	 *            the NodeID of the node where the Chunks have to be restored
	 * @param p_useLiveData
	 *            whether the recover should use current logs or log files
	 * @return whether the operation was successful or not
	 */
	public boolean recover(final short p_owner, final short p_dest, final boolean p_useLiveData) {
		boolean ret = true;

		if (p_dest == m_boot.getNodeID()) {
			if (p_useLiveData) {
				recoverLocally(p_owner);
			} else {
				recoverLocallyFromFile(p_owner);
			}
		} else {
			// #if LOGGER >= INFO
			m_logger.info(RecoveryService.class, "Forwarding recovery to " + NodeID.toHexString(p_dest));
			// #endif /* LOGGER >= INFO */
			final NetworkErrorCodes err = m_network.sendMessage(new RecoverMessage(p_dest, p_owner, p_useLiveData));
			if (err != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(RecoveryService.class,
						"Could not forward command to " + NodeID.toHexString(p_dest) + ". Aborting recovery!");
				// #endif /* LOGGER >= ERROR */
				ret = false;
			}
		}

		return ret;
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {}

	@Override
	protected boolean startService(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {

		m_boot = getComponent(AbstractBootComponent.class);
		m_backup = getComponent(BackupComponent.class);
		m_chunk = getComponent(ChunkComponent.class);
		m_log = getComponent(LogComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		m_network = getComponent(NetworkComponent.class);

		registerNetworkMessages();
		registerNetworkMessageListener();

		// #if LOGGER >= WARN
		if (!m_backup.isActive()) {
			m_logger.warn(RecoveryService.class, "Backup is not activated. Recovery service will not work!");
		}
		// #endif /* LOGGER >= WARN */
		m_backupDirectory = m_backup.getBackupDirectory();

		return true;
	}

	@Override
	protected boolean shutdownService() {
		return true;
	}

	/**
	 * Recovers all Chunks of given node on this node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 */
	private void recoverLocally(final short p_owner) {
		long firstChunkIDOrRangeID;
		short[] backupPeers;
		Chunk[] chunks = null;
		BackupRange[] backupRanges = null;
		RecoverBackupRangeRequest request;

		if (!m_backup.isActive()) {
			// #if LOGGER >= WARN
			m_logger.warn(RecoveryService.class, "Backup is not activated. Cannot recover!");
			// #endif /* LOGGER >= WARN */
		} else {
			backupRanges = m_lookup.getAllBackupRanges(p_owner);
			if (backupRanges != null) {
				for (BackupRange backupRange : backupRanges) {
					backupPeers = backupRange.getBackupPeers();
					firstChunkIDOrRangeID = backupRange.getRangeID();

					// Get Chunks from backup peers (or locally if this is the primary backup peer)
					// for (short backupPeer : backupPeers) {
					// if (backupPeer == m_boot.getNodeID()) {
					// if (ChunkID.getCreatorID(firstChunkIDOrRangeID) == p_owner) {
					// chunks = m_log.recoverBackupRange(p_owner, firstChunkIDOrRangeID, (byte) -1);
					// } else {
					// chunks = m_log.recoverBackupRange(p_owner, -1, (byte) firstChunkIDOrRangeID);
					// }
					// if (chunks == null) {
					// #if LOGGER >= ERROR
					// /*m_logger.error(RecoveryService.class, "Cannot recover Chunks! Trying next backup peer.");*/
					// #endif /* LOGGER >= ERROR */
					// continue;
					// }
					// } else if (backupPeer != -1) {
					// request = new RecoverBackupRangeRequest(backupPeer, p_owner, firstChunkIDOrRangeID);
					// m_network.sendSync(request);
					// chunks = request.getResponse(RecoverBackupRangeResponse.class).getChunks();
					// }
					// break;
					// }

					if (ChunkID.getCreatorID(firstChunkIDOrRangeID) == p_owner) {
						chunks = m_log.recoverBackupRange(p_owner, firstChunkIDOrRangeID, (byte) -1);
					} else {
						chunks = m_log.recoverBackupRange(p_owner, -1, (byte) firstChunkIDOrRangeID);
					}

					// #if LOGGER >= INFO
					m_logger.info(RecoveryService.class, "Retrieved " + chunks.length + " Chunks.");
					// #endif /* LOGGER >= INFO */

					// Store recovered Chunks
					m_chunk.putRecoveredChunks(chunks);

					// Inform superpeers about new location of migrated Chunks (non-migrated Chunks are processed later)
					for (Chunk chunk : chunks) {
						if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
							m_lookup.migrate(chunk.getID(), m_boot.getNodeID());
						}
					}
				}
				// Inform superpeers about new location of non-migrated Chunks
				m_lookup.setRestorerAfterRecovery(p_owner);
			}
		}
	}

	/**
	 * Recovers all Chunks of given node from log file on this node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 */
	private void recoverLocallyFromFile(final short p_owner) {
		String fileName;
		File folderToScan;
		File[] listOfFiles;
		Chunk[] chunks = null;

		if (!m_backup.isActive()) {
			// #if LOGGER >= WARN
			m_logger.warn(RecoveryService.class, "Backup is not activated. Cannot recover!");
			// #endif /* LOGGER >= WARN */
		} else {
			folderToScan = new File(m_backupDirectory);
			listOfFiles = folderToScan.listFiles();
			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					fileName = listOfFiles[i].getName();
					if (fileName.contains("sec" + p_owner)) {
						chunks = m_log.recoverBackupRangeFromFile(fileName, m_backupDirectory);

						if (chunks == null) {
							// #if LOGGER >= ERROR
							m_logger.error(RecoveryService.class, "Cannot recover Chunks! Trying next file.");
							// #endif /* LOGGER >= ERROR */
							continue;
						}
						// #if LOGGER >= INFO
						m_logger.info(RecoveryService.class, "Retrieved " + chunks.length + " Chunks from file.");
						// #endif /* LOGGER >= INFO */

						// Store recovered Chunks
						m_chunk.putRecoveredChunks(chunks);

						if (fileName.contains("M")) {
							// Inform superpeers about new location of migrated Chunks (non-migrated Chunks are
							// processed later)
							for (Chunk chunk : chunks) {
								if (ChunkID.getCreatorID(chunk.getID()) != p_owner) {
									// TODO: This might crash because there is no tree for creator of this chunk
									m_lookup.migrate(chunk.getID(), m_boot.getNodeID());
								}
							}
						}
					}
				}
			}

			// Inform superpeers about new location of non-migrated Chunks
			// TODO: This might crash because there is no tree for recovered peer
			m_lookup.setRestorerAfterRecovery(p_owner);
		}
	}

	/**
	 * Recovers all Chunks of given node on this node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 */
	private int recoverBackupRange(final short p_owner, final long p_firstChunkIDOrRangeID) {
		int ret = 0;
		Chunk[] chunks = null;

		if (ChunkID.getCreatorID(p_firstChunkIDOrRangeID) == p_owner) {
			// Failed peer was the creator -> recover normal backup range
			chunks = m_log.recoverBackupRange(p_owner, p_firstChunkIDOrRangeID, (byte) -1);
			ret = chunks.length;
		} else {
			// Failed peer was not the creator -> recover migration backup range
			chunks = m_log.recoverBackupRange(p_owner, -1, (byte) p_firstChunkIDOrRangeID);
			ret = chunks.length;
		}

		if (ret == 0) {
			// #if LOGGER >= ERROR
			m_logger.error(RecoveryService.class, "Cannot recover Chunks locally.");
			// #endif /* LOGGER >= ERROR */
		}

		// TODO: Not complete!

		return ret;
	}

	/**
	 * Handles an incoming RecoverMessage
	 * @param p_message
	 *            the RecoverMessage
	 */
	private void incomingRecoverMessage(final RecoverMessage p_message) {
		// Outsource recovery to another thread to avoid blocking a message handler
		Runnable task = () -> {
			if (p_message.useLiveData()) {
				recoverLocally(p_message.getOwner());
			} else {
				recoverLocallyFromFile(p_message.getOwner());
			}
		};
		new Thread(task).start();
	}

	/**
	 * Handles an incoming GetChunkIDRequest
	 * @param p_request
	 *            the RecoverBackupRangeRequest
	 */
	private void incomingRecoverBackupRangeRequest(final RecoverBackupRangeRequest p_request) {
		// Outsource recovery to another thread to avoid blocking a message handler
		Runnable task = () -> {
			int recoveredChunks = recoverBackupRange(p_request.getOwner(), p_request.getFirstChunkIDOrRangeID());
			m_network.sendMessage(new RecoverBackupRangeResponse(p_request, recoveredChunks));
		};
		new Thread(task).start();
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case RecoveryMessages.SUBTYPE_RECOVER_MESSAGE:
						incomingRecoverMessage((RecoverMessage) p_message);
						break;
					case RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST:
						incomingRecoverBackupRangeRequest((RecoverBackupRangeRequest) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {
		m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
				RecoveryMessages.SUBTYPE_RECOVER_MESSAGE,
				RecoverMessage.class);
		m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
				RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST,
				RecoverBackupRangeRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.RECOVERY_MESSAGES_TYPE,
				RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE,
				RecoverBackupRangeResponse.class);
	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(RecoverMessage.class, this);
		m_network.register(RecoverBackupRangeRequest.class, this);
	}

}
