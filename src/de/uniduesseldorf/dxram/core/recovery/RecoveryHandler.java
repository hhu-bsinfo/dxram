
package de.uniduesseldorf.dxram.core.recovery;

import java.io.File;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.api.config.NodesConfiguration.Role;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.ChunkHandler.BackupRange;
import de.uniduesseldorf.dxram.core.chunk.ChunkInterface;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.core.recovery.RecoveryMessages.RecoverBackupRangeRequest;
import de.uniduesseldorf.dxram.core.recovery.RecoveryMessages.RecoverBackupRangeResponse;
import de.uniduesseldorf.dxram.core.recovery.RecoveryMessages.RecoverMessage;
import de.uniduesseldorf.dxram.core.util.ChunkID;
import de.uniduesseldorf.dxram.core.util.NodeID;

/**
 * Implements the Recovery-Service
 * @author Florian Klein 06.08.2012
 */
public final class RecoveryHandler implements RecoveryInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(RecoveryHandler.class);
	private static final boolean LOG_ACTIVE = Core.getConfiguration().getBooleanValue(ConfigurationConstants.LOG_ACTIVE);
	private static final String BACKUP_DIRECTORY = Core.getConfiguration().getStringValue(ConfigurationConstants.LOG_DIRECTORY);

	// Attributes
	private NetworkInterface m_network;
	private ChunkInterface m_chunk;
	private LookupInterface m_lookup;
	private LogInterface m_log;

	// Constructors
	/**
	 * Creates an instance of RecoveryHandler
	 */
	public RecoveryHandler() {}

	// Methods
	@Override
	public void initialize() throws DXRAMException {
		LOGGER.trace("Entering initialize");

		m_network = CoreComponentFactory.getNetworkInterface();
		m_network.register(RecoverMessage.class, this);
		m_network.register(RecoverBackupRangeRequest.class, this);

		m_chunk = CoreComponentFactory.getChunkInterface();
		m_lookup = CoreComponentFactory.getLookupInterface();

		if (LOG_ACTIVE && NodeID.getRole().equals(Role.PEER)) {
			m_log = CoreComponentFactory.getLogInterface();
		}

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public void close() {
		LOGGER.trace("Entering close");

		LOGGER.trace("Exiting close");
	}

	@Override
	public boolean recover(final short p_owner, final short p_dest, final boolean p_useLiveData) throws RecoveryException {
		boolean ret = true;

		if (p_dest == NodeID.getLocalNodeID()) {
			if (p_useLiveData) {
				recoverLocally(p_owner);
			} else {
				recoverLocallyFromFile(p_owner);
			}
		} else {
			System.out.println("Forwarding recovery to " + p_dest);
			try {
				new RecoverMessage(p_dest, p_owner, p_useLiveData).send(m_network);
			} catch (final NetworkException e) {
				System.out.println("Could not forward command to " + p_dest + ". Aborting recovery!");
				ret = false;
			}
		}

		return ret;
	}

	/**
	 * Recovers all Chunks of given node on this node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @throws RecoveryException
	 *             if the recovery fails
	 * @throws LookupException
	 *             if the backup peers could not be determined
	 */
	private void recoverLocally(final short p_owner) {
		long firstChunkIDOrRangeID;
		short[] backupPeers;
		Chunk[] chunks = null;
		BackupRange[] backupRanges = null;
		RecoverBackupRangeRequest request;

		try {
			backupRanges = m_lookup.getAllBackupRanges(p_owner);
		} catch (final LookupException e1) {
			System.out.println("Cannot retrieve backup ranges! Aborting.");
		}

		if (backupRanges != null) {
			for (BackupRange backupRange : backupRanges) {
				backupPeers = backupRange.getBackupPeers();
				firstChunkIDOrRangeID = backupRange.getRangeID();

				// Get Chunks from backup peers (or locally if this is the primary backup peer)
				for (short backupPeer : backupPeers) {
					if (backupPeer == NodeID.getLocalNodeID()) {
						try {
							if (ChunkID.getCreatorID(firstChunkIDOrRangeID) == p_owner) {
								chunks = m_log.recoverBackupRange(p_owner, firstChunkIDOrRangeID, (byte) -1);
							} else {
								chunks = m_log.recoverBackupRange(p_owner, -1, (byte) firstChunkIDOrRangeID);
							}
						} catch (final DXRAMException e) {
							System.out.println("Cannot recover Chunks! Trying next backup peer.");
							continue;
						}
					} else if (backupPeer != -1) {
						request = new RecoverBackupRangeRequest(backupPeer, p_owner, firstChunkIDOrRangeID);
						try {
							request.sendSync(m_network);
							chunks = request.getResponse(RecoverBackupRangeResponse.class).getChunks();
						} catch (final NetworkException e) {
							System.out.println("Cannot retrieve Chunks from backup range! Trying next backup peer.");
							continue;
						}
					}
					break;
				}

				try {
					System.out.println("Retrieved " + chunks.length + " Chunks.");

					// Store recovered Chunks
					m_chunk.putRecoveredChunks(chunks);

					// Inform superpeers about new location of migrated Chunks (non-migrated Chunks are processed later)
					for (Chunk chunk : chunks) {
						if (ChunkID.getCreatorID(chunk.getChunkID()) != p_owner) {
							m_lookup.migrate(chunk.getChunkID(), NodeID.getLocalNodeID());
						}
					}
				} catch (final DXRAMException e) {}
			}
			try {
				// Inform superpeers about new location of non-migrated Chunks
				m_lookup.updateAllAfterRecovery(p_owner);
			} catch (final LookupException e) {}
		}
	}

	/**
	 * Recovers all Chunks of given node from log file on this node
	 * @param p_owner
	 *            the NodeID of the node whose Chunks have to be restored
	 * @throws RecoveryException
	 *             if the recovery fails
	 * @throws LookupException
	 *             if the backup peers could not be determined
	 */
	private void recoverLocallyFromFile(final short p_owner) {
		String fileName;
		File folderToScan;
		File[] listOfFiles;
		Chunk[] chunks = null;

		folderToScan = new File(BACKUP_DIRECTORY);
		listOfFiles = folderToScan.listFiles();
		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				fileName = listOfFiles[i].getName();
				if (fileName.contains("sec" + p_owner)) {
					try {
						chunks = m_log.recoverBackupRangeFromFile(fileName, BACKUP_DIRECTORY);
					} catch (final DXRAMException e) {
						System.out.println("Cannot recover Chunks! Trying next backup peer.");
						continue;
					}

					try {
						System.out.println("Retrieved " + chunks.length + " Chunks from file.");

						// Store recovered Chunks
						m_chunk.putRecoveredChunks(chunks);

						if (fileName.contains("M")) {
							// Inform superpeers about new location of migrated Chunks (non-migrated Chunks are processed later)
							for (Chunk chunk : chunks) {
								if (ChunkID.getCreatorID(chunk.getChunkID()) != p_owner) {
									// TODO: This might crash because there is no tree for creator of this chunk
									m_lookup.migrate(chunk.getChunkID(), NodeID.getLocalNodeID());
								}
							}
						}
					} catch (final DXRAMException e) {}
				}
			}
		}

		try {
			// Inform superpeers about new location of non-migrated Chunks
			// TODO: This might crash because there is no tree for recovered peer
			m_lookup.updateAllAfterRecovery(p_owner);
		} catch (final LookupException e) {}
	}

	/**
	 * Handles an incoming RecoverMessage
	 * @param p_message
	 *            the RecoverMessage
	 */
	private void incomingRecoverMessage(final RecoverMessage p_message) {
		if (p_message.useLiveData()) {
			recoverLocally(p_message.getOwner());
		} else {
			recoverLocallyFromFile(p_message.getOwner());
		}
	}

	/**
	 * Handles an incoming GetChunkIDRequest
	 * @param p_request
	 *            the RecoverBackupRangeRequest
	 */
	private void incomingRecoverBackupRangeRequest(final RecoverBackupRangeRequest p_request) {
		short owner;
		long firstChunkIDOrRangeID;
		Chunk[] chunks = null;

		LOGGER.trace("Got request: RECOVER_REQUEST from " + p_request.getSource());

		owner = p_request.getOwner();
		firstChunkIDOrRangeID = p_request.getFirstChunkIDOrRangeID();
		try {
			if (ChunkID.getCreatorID(firstChunkIDOrRangeID) == owner) {
				chunks = m_log.recoverBackupRange(owner, firstChunkIDOrRangeID, (byte) -1);
			} else {
				chunks = m_log.recoverBackupRange(owner, -1, (byte) firstChunkIDOrRangeID);
			}
		} catch (final DXRAMException e) {
			System.out.println("Cannot recover Chunks!");
		}

		try {
			new RecoverBackupRangeResponse(p_request, chunks).send(m_network);
		} catch (final NetworkException e) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == RecoveryMessages.TYPE) {
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

	@Override
	public void triggerEvent(final ConnectionLostEvent p_event) {}
}
