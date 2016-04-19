
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupRangeWithBackupPeers;
import de.hhu.bsinfo.dxram.lookup.event.NameserviceCacheEntryUpdateEvent;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetAllBackupRangesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetAllBackupRangesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetChunkIDForNameserviceEntryRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetChunkIDForNameserviceEntryResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupRangeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntriesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntriesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntryCountRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetNameserviceEntryCountResponse;
import de.hhu.bsinfo.dxram.lookup.messages.InitRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.InitRangeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.InsertNameserviceEntriesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.InsertNameserviceEntriesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.JoinRequest;
import de.hhu.bsinfo.dxram.lookup.messages.JoinResponse;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateRangeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateRequest;
import de.hhu.bsinfo.dxram.lookup.messages.MigrateResponse;
import de.hhu.bsinfo.dxram.lookup.messages.NameserviceUpdatePeerCachesMessage;
import de.hhu.bsinfo.dxram.lookup.messages.PingSuperpeerMessage;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SendSuperpeersMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SetRestorerAfterRecoveryMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.CRC16;
import de.hhu.bsinfo.utils.Pair;

/**
 * Peer functionality for overlay
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class OverlayPeer implements MessageReceiver {

	private static final short OPEN_INTERVAL = 2;
	private static final boolean IS_NOT_SUPERPEER = false;
	private static final boolean NO_CHECK = false;
	private static final boolean BACKUP = true;
	private static final boolean NO_BACKUP = false;

	// Attributes
	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private NetworkComponent m_network;
	private EventComponent m_event;

	private short m_nodeID = -1;
	private short m_mySuperpeer = -1;
	private ArrayList<Short> m_superpeers;
	private int m_initialNumberOfSuperpeers;
	private ReentrantLock m_overlayLock;

	private CRC16 m_hashGenerator = new CRC16();

	/**
	 * Creates an instance of OverlayPeer
	 * @param p_nodeID
	 *            the own NodeID
	 * @param p_contactSuperpeer
	 *            the superpeer to contact for joining
	 * @param p_initialNumberOfSuperpeers
	 *            the number of expeced superpeers
	 * @param p_boot
	 *            the BootComponent
	 * @param p_logger
	 *            the LoggerComponent
	 * @param p_network
	 *            the NetworkComponent
	 * @param p_event
	 *            the EventComponent
	 */
	public OverlayPeer(final short p_nodeID, final short p_contactSuperpeer, final int p_initialNumberOfSuperpeers,
			final AbstractBootComponent p_boot, final LoggerComponent p_logger, final NetworkComponent p_network,
			final EventComponent p_event) {
		m_boot = p_boot;
		m_logger = p_logger;
		m_network = p_network;
		m_event = p_event;

		m_nodeID = m_boot.getNodeID();

		registerNetworkMessages();
		registerNetworkMessageListener();

		m_overlayLock = new ReentrantLock(false);
		joinSuperpeerOverlay(p_contactSuperpeer);
	}

	/* Lookup */

	/**
	 * Get the corresponding LookupRange for the given ChunkID
	 * @param p_chunkID
	 *            the ChunkID
	 * @return the current location and the range borders
	 */
	public LookupRange getLookupRange(final long p_chunkID) {
		LookupRange ret = null;
		short nodeID;
		short responsibleSuperpeer;
		boolean check = false;

		GetLookupRangeRequest request;
		GetLookupRangeResponse response;

		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}
		nodeID = ChunkID.getCreatorID(p_chunkID);
		// FIXME will not terminate if chunk id requested does not exist
		// while (null == ret) {
		responsibleSuperpeer = getResponsibleSuperpeer(nodeID, check);

		if (-1 != responsibleSuperpeer) {
			request = new GetLookupRangeRequest(responsibleSuperpeer, p_chunkID);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				// Responsible superpeer is not available, try again and check responsible superpeer
				check = true;
			}

			response = request.getResponse(GetLookupRangeResponse.class);

			ret = response.getLookupRange();
		}
		// }

		return ret;
	}

	/**
	 * Remove the ChunkIDs from range after deletion of that chunks
	 * @param p_chunkIDs
	 *            the ChunkIDs
	 */
	public void removeChunkIDs(final long[] p_chunkIDs) {
		short responsibleSuperpeer;
		short[] backupSuperpeers;

		RemoveChunkIDsRequest request;
		RemoveChunkIDsResponse response;

		while (true) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new RemoveChunkIDsRequest(responsibleSuperpeer, p_chunkIDs, NO_BACKUP);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}

			response = request.getResponse(RemoveChunkIDsResponse.class);

			backupSuperpeers = response.getBackupSuperpeers();
			if (null != backupSuperpeers) {
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new RemoveChunkIDsRequest(backupSuperpeers[i], p_chunkIDs, BACKUP);
						if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
							// Ignore superpeer failure, own superpeer will fix this
							continue;
						}
					}
				}
				break;
			}
		}
	}

	/* Name Service */

	/**
	 * Insert a new name service entry
	 * @param p_id
	 *            the AID
	 * @param p_chunkID
	 *            the ChunkID
	 */
	public void insertNameserviceEntry(final int p_id, final long p_chunkID) {
		short responsibleSuperpeer;
		short[] backupSuperpeers;
		boolean check = false;
		InsertNameserviceEntriesRequest request;
		InsertNameserviceEntriesResponse response;

		// Insert ChunkID <-> ApplicationID mapping
		assert p_id < Math.pow(2, 31) && p_id >= 0;

		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}
		while (true) {
			responsibleSuperpeer = getResponsibleSuperpeer(m_hashGenerator.hash(p_id), check);

			if (-1 != responsibleSuperpeer) {
				request = new InsertNameserviceEntriesRequest(responsibleSuperpeer, p_id, p_chunkID, NO_BACKUP);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
					continue;
				}

				response = request.getResponse(InsertNameserviceEntriesResponse.class);

				backupSuperpeers = response.getBackupSuperpeers();
				if (null != backupSuperpeers) {
					if (-1 != backupSuperpeers[0]) {
						// Send backups
						for (int i = 0; i < backupSuperpeers.length; i++) {
							request = new InsertNameserviceEntriesRequest(backupSuperpeers[i], p_id, p_chunkID, BACKUP);
							if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
								// Ignore superpeer failure, own superpeer will fix this
								continue;
							}
						}
					}
					break;
				}
			}
		}
	}

	/**
	 * Get ChunkID for give nameservice id. Use this if you assume
	 * that your entry has to exist.
	 * @param p_id
	 *            the nameservice id
	 * @param p_timeoutMs
	 *            Timeout for trying to get the entry (if it does not exist, yet).
	 *            set this to -1 for infinite loop if you know for sure, that the entry has to exist
	 * @return the corresponding ChunkID
	 */
	public long getChunkIDForNameserviceEntry(final int p_id, final int p_timeoutMs) {
		long ret = -1;
		short responsibleSuperpeer;
		boolean check = false;
		GetChunkIDForNameserviceEntryRequest request;

		// Resolve ChunkID <-> ApplicationID mapping to return corresponding ChunkID
		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}
		long start = System.currentTimeMillis();
		do {
			responsibleSuperpeer = getResponsibleSuperpeer(m_hashGenerator.hash(p_id), check);

			if (-1 != responsibleSuperpeer) {
				request = new GetChunkIDForNameserviceEntryRequest(responsibleSuperpeer, p_id);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
					continue;
				}

				ret = request.getResponse(GetChunkIDForNameserviceEntryResponse.class).getChunkID();
				// 0 is considered invalid, but outside of this scope, we always consider -1 as invalid
				if (ret == 0) {
					ret = ChunkID.INVALID_ID;
				}
			}
		} while (p_timeoutMs == -1 || System.currentTimeMillis() - start < p_timeoutMs);

		return ret;
	}

	/**
	 * Get the number of entries in name service
	 * @return the number of name service entries
	 */
	public int getNameserviceEntryCount() {
		int ret = 0;
		Short[] superpeers;
		GetNameserviceEntryCountRequest request;
		GetNameserviceEntryCountResponse response;

		m_overlayLock.lock();
		superpeers = m_superpeers.toArray(new Short[m_superpeers.size()]);
		m_overlayLock.unlock();

		for (short superpeer : superpeers) {
			request = new GetNameserviceEntryCountRequest(superpeer);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(), "Could not determine nameservice entry count");
				ret = -1;
				break;
			} else {
				response = request.getResponse(GetNameserviceEntryCountResponse.class);
				ret += response.getCount();
			}
		}

		return ret;
	}

	/**
	 * Get all available nameservice entries.
	 * @return List of nameservice entries or null on error;
	 */
	public ArrayList<Pair<Integer, Long>> getNameserviceEntries() {
		ArrayList<Pair<Integer, Long>> entries = new ArrayList<Pair<Integer, Long>>();
		Short[] superpeers;
		GetNameserviceEntriesRequest request;
		GetNameserviceEntriesResponse response;

		m_overlayLock.lock();
		superpeers = m_superpeers.toArray(new Short[m_superpeers.size()]);
		m_overlayLock.unlock();

		for (short superpeer : superpeers) {
			request = new GetNameserviceEntriesRequest(superpeer);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(), "Could not determine nameservice entries");
				entries = null;
				break;
			} else {
				response = request.getResponse(GetNameserviceEntriesResponse.class);
				ArrayList<Pair<Integer, Long>> tmpEntries = response.getEntries();
				entries.addAll(tmpEntries);
			}
		}

		return entries;
	}

	/* Migration */

	/**
	 * Store migration of given ChunkID to a new location
	 * @param p_chunkID
	 *            the ChunkID
	 * @param p_nodeID
	 *            the new owner
	 */
	public void migrate(final long p_chunkID, final short p_nodeID) {
		short responsibleSuperpeer;
		boolean finished = false;

		MigrateRequest request;

		while (!finished) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new MigrateRequest(responsibleSuperpeer, p_chunkID, p_nodeID, NO_BACKUP);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}
			finished = request.getResponse(MigrateResponse.class).getStatus();
		}
	}

	/**
	 * Store migration of a range of ChunkIDs to a new location
	 * @param p_startCID
	 *            the first ChunkID
	 * @param p_endCID
	 *            the last ChunkID
	 * @param p_nodeID
	 *            the new owner
	 */
	public void migrateRange(final long p_startCID, final long p_endCID, final short p_nodeID) {
		short creator;
		short responsibleSuperpeer;
		boolean finished = false;

		MigrateRangeRequest request;

		creator = ChunkID.getCreatorID(p_startCID);
		if (creator != ChunkID.getCreatorID(p_endCID)) {
			m_logger.error(getClass(), "Start and end object's creators not equal");
		} else {
			while (!finished) {
				responsibleSuperpeer = m_mySuperpeer;

				request = new MigrateRangeRequest(responsibleSuperpeer, p_startCID, p_endCID, p_nodeID, NO_BACKUP);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException e1) {}
					continue;
				}

				finished = request.getResponse(MigrateRangeResponse.class).getStatus();
			}
		}
	}

	/* Backup */

	/**
	 * Initialize a new backup range
	 * @param p_firstChunkIDOrRangeID
	 *            the RangeID or ChunkID of the first chunk in range
	 * @param p_primaryAndBackupPeers
	 *            the creator and all backup peers
	 */
	public void initRange(final long p_firstChunkIDOrRangeID,
			final LookupRangeWithBackupPeers p_primaryAndBackupPeers) {
		short responsibleSuperpeer;
		boolean finished = false;

		InitRangeRequest request;

		while (!finished) {
			responsibleSuperpeer = m_mySuperpeer;

			request = new InitRangeRequest(responsibleSuperpeer, p_firstChunkIDOrRangeID,
					p_primaryAndBackupPeers.convertToLong(), NO_BACKUP);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}

			finished = request.getResponse(InitRangeResponse.class).getStatus();
		}
	}

	/**
	 * Get all backup ranges for given node
	 * @param p_nodeID
	 *            the NodeID
	 * @return all backup ranges for given node
	 */
	public BackupRange[] getAllBackupRanges(final short p_nodeID) {
		BackupRange[] ret = null;
		short responsibleSuperpeer;
		boolean check = false;

		GetAllBackupRangesRequest request;
		GetAllBackupRangesResponse response;

		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}
		while (null == ret) {
			responsibleSuperpeer = getResponsibleSuperpeer(p_nodeID, check);

			if (-1 != responsibleSuperpeer) {
				request = new GetAllBackupRangesRequest(responsibleSuperpeer, p_nodeID);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again and check responsible superpeer
					check = true;
					continue;
				}
				response = request.getResponse(GetAllBackupRangesResponse.class);
				ret = response.getBackupRanges();
			}
		}

		return ret;
	}

	/* Recovery */

	/**
	 * Set restorer as new creator for recovered chunks
	 * @param p_owner
	 *            NodeID of the recovered peer
	 */
	public void setRestorerAfterRecovery(final short p_owner) {
		short responsibleSuperpeer;
		boolean check = false;

		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}
		while (true) {
			responsibleSuperpeer = getResponsibleSuperpeer(p_owner, check);

			if (m_network.sendMessage(
					new SetRestorerAfterRecoveryMessage(responsibleSuperpeer, p_owner)) != NetworkErrorCodes.SUCCESS) {
				// Responsible superpeer is not available, try again (superpeers will be updated
				// automatically by network thread)
				try {
					Thread.sleep(1000);
				} catch (final InterruptedException e1) {}
				continue;
			}

			break;
		}
	}

	/**
	 * Checks if all superpeers are offline
	 * @return if all superpeers are offline
	 */
	public boolean allSuperpeersDown() {
		boolean ret = true;
		short superpeer;
		int i = 0;

		if (m_network.sendMessage(new PingSuperpeerMessage(m_mySuperpeer)) != NetworkErrorCodes.SUCCESS) {
			if (!m_superpeers.isEmpty()) {
				while (i < m_superpeers.size()) {
					superpeer = m_superpeers.get(i++);
					if (m_network.sendMessage(new PingSuperpeerMessage(superpeer)) != NetworkErrorCodes.SUCCESS) {
						continue;
					}

					ret = false;
					break;
				}
			}
		} else {
			ret = false;
		}

		return ret;
	}

	/**
	 * Joins the superpeer overlay through contactSuperpeer
	 * @param p_contactSuperpeer
	 *            NodeID of a known superpeer
	 * @return whether joining was successful
	 */
	private boolean joinSuperpeerOverlay(final short p_contactSuperpeer) {
		short contactSuperpeer;
		JoinRequest joinRequest = null;
		JoinResponse joinResponse = null;

		m_logger.trace(getClass(), "Entering joinSuperpeerOverlay with: p_contactSuperpeer=" + p_contactSuperpeer);

		contactSuperpeer = p_contactSuperpeer;

		if (p_contactSuperpeer == NodeID.INVALID_ID) {
			m_logger.error(getClass(), "Cannot join superpeer overlay, no bootstrap superpeer available to contact.");
			return false;
		}

		while (-1 != contactSuperpeer) {
			m_logger.trace(getClass(),
					"Contacting " + contactSuperpeer + " to get the responsible superpeer, I am "
							+ NodeID.toHexString(m_nodeID));

			joinRequest = new JoinRequest(contactSuperpeer, m_nodeID, IS_NOT_SUPERPEER);
			if (m_network.sendSync(joinRequest) != NetworkErrorCodes.SUCCESS) {
				// Contact superpeer is not available, get a new contact superpeer
				contactSuperpeer = m_boot.getNodeIDBootstrap();
				continue;
			}

			joinResponse = joinRequest.getResponse(JoinResponse.class);
			contactSuperpeer = joinResponse.getNewContactSuperpeer();
		}
		m_superpeers = joinResponse.getSuperpeers();
		m_mySuperpeer = joinResponse.getSource();
		OverlayHelper.insertSuperpeer(m_mySuperpeer, m_superpeers);

		m_logger.trace(getClass(), "Exiting joinSuperpeerOverlay");

		return true;
	}

	/**
	 * Determines the responsible superpeer for given NodeID
	 * @param p_nodeID
	 *            NodeID from chunk whose location is searched
	 * @param p_check
	 *            whether the result has to be checked (in case of incomplete superpeer overlay) or not
	 * @return the responsible superpeer for given ChunkID
	 */
	private short getResponsibleSuperpeer(final short p_nodeID, final boolean p_check) {
		short responsibleSuperpeer = -1;
		short predecessor;
		short hisSuccessor;
		int index;
		AskAboutSuccessorRequest request = null;
		AskAboutSuccessorResponse response = null;

		m_logger.trace(OverlayHelper.class,
				"Entering getResponsibleSuperpeer with: p_nodeID=" + NodeID.toHexString(p_nodeID));

		m_overlayLock.lock();
		if (!m_superpeers.isEmpty()) {
			index = Collections.binarySearch(m_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_superpeers.size()) {
					index = 0;
				}
			}
			responsibleSuperpeer = m_superpeers.get(index);

			if (p_check && 1 < m_superpeers.size()) {
				if (0 == index) {
					index = m_superpeers.size() - 1;
				} else {
					index--;
				}
				predecessor = m_superpeers.get(index);
				m_overlayLock.unlock();

				while (true) {
					request = new AskAboutSuccessorRequest(predecessor);
					if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
						// Predecessor is not available, try responsibleSuperpeer without checking
						break;
					}

					response = request.getResponse(AskAboutSuccessorResponse.class);
					hisSuccessor = response.getSuccessor();
					if (responsibleSuperpeer == hisSuccessor) {
						break;
					} else if (OverlayHelper.isNodeInRange(p_nodeID, predecessor, hisSuccessor, OPEN_INTERVAL)) {
						responsibleSuperpeer = hisSuccessor;
						break;
					} else {
						predecessor = hisSuccessor;
					}
				}
			} else {
				m_overlayLock.unlock();
			}
		} else {
			m_logger.warn(OverlayHelper.class, "do not know any superpeer");
			m_overlayLock.unlock();
		}
		m_logger.trace(OverlayHelper.class, "Exiting getResponsibleSuperpeer");

		return responsibleSuperpeer;
	}

	/**
	 * Handles an incoming SendSuperpeersMessage
	 * @param p_sendSuperpeersMessage
	 *            the SendSuperpeersMessage
	 */
	private void incomingSendSuperpeersMessage(final SendSuperpeersMessage p_sendSuperpeersMessage) {
		short source;

		source = p_sendSuperpeersMessage.getSource();
		m_logger.trace(getClass(), "Got Message: SEND_SUPERPEERS_MESSAGE from " + NodeID.toHexString(source));

		m_overlayLock.lock();
		m_superpeers = p_sendSuperpeersMessage.getSuperpeers();
		OverlayHelper.insertSuperpeer(source, m_superpeers);
		m_overlayLock.unlock();

		if (m_mySuperpeer != source) {
			if (source == getResponsibleSuperpeer(m_nodeID, NO_CHECK)) {
				m_mySuperpeer = source;
			}
		}
	}

	/**
	 * Handles an incoming NameserviceUpdatePeerCachesMessage
	 * @param p_message
	 *            the NameserviceUpdatePeerCachesMessage
	 */
	private void incomingNameserviceUpdatePeerCachesMessage(final NameserviceUpdatePeerCachesMessage p_message) {
		m_event.fireEvent(new NameserviceCacheEntryUpdateEvent(getClass().getSimpleName(), p_message.getID(),
				p_message.getChunkID()));
	}

	/**
	 * Handles an incoming Message
	 * @param p_message
	 *            the Message
	 */
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE:
						incomingSendSuperpeersMessage((SendSuperpeersMessage) p_message);
						break;
					case LookupMessages.SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE:
						incomingNameserviceUpdatePeerCachesMessage((NameserviceUpdatePeerCachesMessage) p_message);
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
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_JOIN_REQUEST, JoinRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_JOIN_RESPONSE, JoinResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST,
				GetLookupRangeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_RESPONSE,
				GetLookupRangeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST,
				RemoveChunkIDsRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_RESPONSE,
				RemoveChunkIDsResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST,
				InsertNameserviceEntriesRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_RESPONSE,
				InsertNameserviceEntriesResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE,
				LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST,
				GetChunkIDForNameserviceEntryRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE,
				LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE,
				GetChunkIDForNameserviceEntryResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST,
				GetNameserviceEntryCountRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE,
				GetNameserviceEntryCountResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST,
				GetNameserviceEntriesRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_RESPONSE,
				GetNameserviceEntriesResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE,
				LookupMessages.SUBTYPE_NAMESERVICE_UPDATE_PEER_CACHES_MESSAGE,
				NameserviceUpdatePeerCachesMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_REQUEST,
				MigrateRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RESPONSE,
				MigrateResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST,
				MigrateRangeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE,
				MigrateRangeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST,
				InitRangeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE,
				InitRangeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST,
				GetAllBackupRangesRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_RESPONSE,
				GetAllBackupRangesResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE,
				SetRestorerAfterRecoveryMessage.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE,
				PingSuperpeerMessage.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST,
				AskAboutSuccessorRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE,
				AskAboutSuccessorResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE,
				SendSuperpeersMessage.class);
	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(SendSuperpeersMessage.class, this);
		m_network.register(NameserviceUpdatePeerCachesMessage.class, this);
	}

}
