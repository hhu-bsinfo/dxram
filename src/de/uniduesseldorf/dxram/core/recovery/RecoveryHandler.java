
package de.uniduesseldorf.dxram.core.recovery;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.CoreComponentFactory;
import de.uniduesseldorf.dxram.core.api.ChunkID;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.chunk.Chunk;
import de.uniduesseldorf.dxram.core.chunk.ChunkHandler.BackupRange;
import de.uniduesseldorf.dxram.core.events.ConnectionLostListener;
import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;
import de.uniduesseldorf.dxram.core.exceptions.LookupException;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.exceptions.RecoveryException;
import de.uniduesseldorf.dxram.core.log.LogInterface;
import de.uniduesseldorf.dxram.core.lookup.LookupInterface;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages;
import de.uniduesseldorf.dxram.core.net.AbstractMessage;
import de.uniduesseldorf.dxram.core.net.NetworkInterface;
import de.uniduesseldorf.dxram.core.net.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.dxram.core.recovery.RecoveryMessages.RecoverBackupRangeRequest;
import de.uniduesseldorf.dxram.core.recovery.RecoveryMessages.RecoverBackupRangeResponse;

/**
 * Implements the Recovery-Service
 * @author Florian Klein 06.08.2012
 */
public final class RecoveryHandler implements RecoveryInterface, MessageReceiver, ConnectionLostListener {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(RecoveryHandler.class);

	// Attributes
	private NetworkInterface m_network;
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
		m_network.register(RecoverBackupRangeRequest.class, this);

		m_lookup = CoreComponentFactory.getLookupInterface();
		m_log = CoreComponentFactory.getLogInterface();

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public void close() {
		LOGGER.trace("Entering close");

		LOGGER.trace("Exiting close");
	}

	@Override
	public Chunk[] recover(final short p_nodeID) throws RecoveryException {
		Chunk[] ret = null;
		long firstChunkIDOrRangeID;
		short[] backupPeers;
		BackupRange[] backupRanges = null;
		RecoverBackupRangeRequest request;

		try {
			backupRanges = m_lookup.getAllBackupRanges(p_nodeID);
		} catch (final LookupException e1) {
			System.out.println("Cannot retrieve backup ranges! Aborting.");
		}

		if (backupRanges != null) {
			for (BackupRange backupRange : backupRanges) {
				backupPeers = backupRange.getBackupPeers();
				firstChunkIDOrRangeID = backupRange.getRangeID();
				for (short backupPeer : backupPeers) {
					if (backupPeer == NodeID.getLocalNodeID()) {
						try {
							if (ChunkID.getCreatorID(firstChunkIDOrRangeID) == p_nodeID) {
								ret = m_log.recoverBackupRange(p_nodeID, firstChunkIDOrRangeID, (byte) -1);
							} else {
								ret = m_log.recoverBackupRange(p_nodeID, -1, (byte) firstChunkIDOrRangeID);
							}
						} catch (final DXRAMException e) {
							System.out.println("Cannot recover Chunks! Trying next backup peer.");
							continue;
						}
					} else if (backupPeer != -1) {
						request = new RecoverBackupRangeRequest(backupPeer, p_nodeID, firstChunkIDOrRangeID);
						try {
							request.sendSync(m_network);
							ret = request.getResponse(RecoverBackupRangeResponse.class).getChunks();
						} catch (final NetworkException e) {
							System.out.println("Cannot retrieve Chunks from backup range! Trying next backup peer.");
							continue;
						}
					}
					break;
				}
			}
		}

		return ret;
	}

	/**
	 * Handles an incoming GetChunkIDRequest
	 * @param p_request
	 *            the RecoverBackupRangeRequest
	 */
	private void incomingRecoverRequest(final RecoverBackupRangeRequest p_request) {
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
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
				case RecoveryMessages.SUBTYPE_RECOVER_REQUEST:
					incomingRecoverRequest((RecoverBackupRangeRequest) p_message);
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
