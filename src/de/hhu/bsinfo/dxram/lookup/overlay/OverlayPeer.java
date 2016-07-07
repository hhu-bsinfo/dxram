
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupRangeWithBackupPeers;
import de.hhu.bsinfo.dxram.lookup.event.NameserviceCacheEntryUpdateEvent;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierAllocRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierAllocResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierChangeSizeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierChangeSizeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierFreeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierFreeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierGetStatusRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierGetStatusResponse;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierReleaseMessage;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierSignOnRequest;
import de.hhu.bsinfo.dxram.lookup.messages.BarrierSignOnResponse;
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
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageCreateRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageCreateResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageGetRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageGetResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStoragePutRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStoragePutResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageRemoveMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageStatusRequest;
import de.hhu.bsinfo.dxram.lookup.messages.SuperpeerStorageStatusResponse;
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

		m_initialNumberOfSuperpeers = p_initialNumberOfSuperpeers;

		m_nodeID = p_nodeID;

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
				} catch (final InterruptedException ignored) {}
				continue;
			}

			response = request.getResponse(RemoveChunkIDsResponse.class);

			backupSuperpeers = response.getBackupSuperpeers();
			if (null != backupSuperpeers) {
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (short backupSuperpeer : backupSuperpeers) {
						request = new RemoveChunkIDsRequest(backupSuperpeer, p_chunkIDs, BACKUP);
						if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
							// Ignore superpeer failure, own superpeer will fix this
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
					} catch (final InterruptedException ignored) {}
					continue;
				}

				response = request.getResponse(InsertNameserviceEntriesResponse.class);

				backupSuperpeers = response.getBackupSuperpeers();
				if (null != backupSuperpeers) {
					if (-1 != backupSuperpeers[0]) {
						// Send backups
						for (short backupSuperpeer : backupSuperpeers) {
							request = new InsertNameserviceEntriesRequest(backupSuperpeer, p_id, p_chunkID, BACKUP);
							if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
								// Ignore superpeer failure, own superpeer will fix this
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
					} catch (final InterruptedException ignored) {}
					continue;
				}

				ret = request.getResponse(GetChunkIDForNameserviceEntryResponse.class).getChunkID();
				// 0 is considered invalid, but outside of this scope, we always consider -1 as invalid
				if (ret == 0) {
					ret = ChunkID.INVALID_ID;
				} else {
					// valid, we are done
					break;
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
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Could not determine nameservice entry count");
				// #endif /* LOGGER >= ERROR */
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
		ArrayList<Pair<Integer, Long>> entries = new ArrayList<>();
		Short[] superpeers;
		GetNameserviceEntriesRequest request;
		GetNameserviceEntriesResponse response;

		m_overlayLock.lock();
		superpeers = m_superpeers.toArray(new Short[m_superpeers.size()]);
		m_overlayLock.unlock();

		for (short superpeer : superpeers) {
			request = new GetNameserviceEntriesRequest(superpeer);
			if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Could not determine nameservice entries");
				// #endif /* LOGGER >= ERROR */
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
				} catch (final InterruptedException ignored) {}
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
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Start and end object's creators not equal");
			// #endif /* LOGGER >= ERROR */
		} else {
			while (!finished) {
				responsibleSuperpeer = m_mySuperpeer;

				request = new MigrateRangeRequest(responsibleSuperpeer, p_startCID, p_endCID, p_nodeID, NO_BACKUP);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {}
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
				} catch (final InterruptedException ignored) {}
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
				} catch (final InterruptedException ignored) {}
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
	 * Allocate a new barrier.
	 * @param p_size
	 *            Size of the barrier (i.e. number of peers that have to sign on).
	 * @return Id of the barrier allocated or -1 on failure.
	 */
	public int barrierAllocate(final int p_size) {
		// the superpeer responsible for the peer will be the storage for this barrier
		BarrierAllocRequest request = new BarrierAllocRequest(m_mySuperpeer, p_size);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Allocating barrier with size " + p_size + " on superpeer " + NodeID.toHexString(m_mySuperpeer)
							+ " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			return BarrierID.INVALID_ID;
		}

		BarrierAllocResponse response = (BarrierAllocResponse) request.getResponse();
		return response.getBarrierId();
	}

	/**
	 * Free an allocate barrier.
	 * @param p_barrierId
	 *            Id of the barrier to free.
	 * @return True if successful, false otherwise.
	 */
	public boolean barrierFree(final int p_barrierId) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return false;
		}

		short responsibleSuperpeer = BarrierID.getOwnerID(p_barrierId);
		BarrierFreeRequest message = new BarrierFreeRequest(responsibleSuperpeer, p_barrierId);
		NetworkErrorCodes err = m_network.sendSync(message);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Freeing barrier " + BarrierID.toHexString(p_barrierId) + " on superpeer " + NodeID
							.toHexString(responsibleSuperpeer) + " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		BarrierFreeResponse response = (BarrierFreeResponse) message.getResponse();
		if (response.getStatusCode() == -1) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Freeing barrier " + BarrierID.toHexString(p_barrierId) + " on superpeer " + NodeID
							.toHexString(responsibleSuperpeer) + " failed: barrier does not exist.");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		return true;
	}

	/**
	 * Alter the size of an existing barrier (i.e. you want to keep the barrier id but with a different size).
	 * @param p_barrierId
	 *            Id of an allocated barrier to change the size of.
	 * @param p_size
	 *            New size for the barrier.
	 * @return True if changing size was successful, false otherwise.
	 */
	public boolean barrierChangeSize(final int p_barrierId, final int p_size) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return false;
		}

		short responsibleSuperpeer = BarrierID.getOwnerID(p_barrierId);
		BarrierChangeSizeRequest request = new BarrierChangeSizeRequest(responsibleSuperpeer, p_barrierId, p_size);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Sending barrier change size request to superpeer " + NodeID.toHexString(responsibleSuperpeer)
							+ " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		BarrierChangeSizeResponse response = (BarrierChangeSizeResponse) request.getResponse();
		// #if LOGGER >= ERROR
		if (response.getStatusCode() != 0) {
			m_logger.error(getClass(), "Changing size of barrier " + BarrierID.toHexString(p_barrierId) + " failed.");
		}
		// #endif /* LOGGER >= ERROR */

		return response.getStatusCode() == 0;
	}

	/**
	 * Sign on to a barrier and wait for it getting released (number of peers, barrier size, have signed on).
	 * @param p_barrierId
	 *            Id of the barrier to sign on to.
	 * @param p_customData
	 *            Custom data to pass along with the sign on
	 * @return A pair consisting of the list of signed on peers and their custom data passed along
	 *         with the sign ons, null on error
	 */
	public Pair<short[], long[]> barrierSignOn(final int p_barrierId, final long p_customData) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return null;
		}

		Semaphore waitForRelease = new Semaphore(0);
		final BarrierReleaseMessage[] releaseMessage = {null};
		MessageReceiver msg = p_message -> {
			if (p_message != null) {
				if (p_message.getType() == LookupMessages.TYPE) {
					switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE: {
						releaseMessage[0] = (BarrierReleaseMessage) p_message;
						if (releaseMessage[0].getBarrierId() == p_barrierId) {
							waitForRelease.release();
						}
						break;
					}
					default:
						break;
					}
				}
			}
		};

		// make sure to register the listener BEFORE sending the sign on to not miss the release message
		m_network.register(BarrierReleaseMessage.class, msg);

		short responsibleSuperpeer = BarrierID.getOwnerID(p_barrierId);
		BarrierSignOnRequest request = new BarrierSignOnRequest(responsibleSuperpeer, p_barrierId, p_customData);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Sign on barrier " + BarrierID.toHexString(p_barrierId) + " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			m_network.unregister(BarrierReleaseMessage.class, msg);
			return null;
		}

		BarrierSignOnResponse response = (BarrierSignOnResponse) request.getResponse();
		if (response.getBarrierId() != p_barrierId || response.getStatusCode() != 0) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Sign on barrier " + BarrierID.toHexString(p_barrierId) + " failed.");
			// #endif /* LOGGER >= ERROR */
			m_network.unregister(BarrierReleaseMessage.class, msg);
			return null;
		}

		try {
			waitForRelease.acquire();
		} catch (final InterruptedException ignored) {}

		m_network.unregister(BarrierReleaseMessage.class, msg);

		return new Pair<>(releaseMessage[0].getSignedOnPeers(), releaseMessage[0].getCustomData());
	}

	/**
	 * Get the status of a barrier.
	 * @param p_barrierId
	 *            Id of the barrier.
	 * @return Short array with currently signed on peers with the first index being the number of signed on peers
	 */
	public short[] barrierGetStatus(final int p_barrierId) {
		if (p_barrierId == BarrierID.INVALID_ID) {
			return null;
		}

		BarrierGetStatusRequest request = new BarrierGetStatusRequest(m_mySuperpeer, p_barrierId);
		NetworkErrorCodes err = m_network.sendSync(request);
		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Getting status request of barrier " + BarrierID.toHexString(p_barrierId) + " failed: " + err);
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		BarrierGetStatusResponse response = (BarrierGetStatusResponse) request.getResponse();
		if (response.getStatusCode() == -1) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Getting status request of barrier " + BarrierID.toHexString(p_barrierId)
					+ " failed: barrier does not exist");
			// #endif /* LOGGER >= ERROR */
			return null;
		}

		return response.getBarrierStatus();
	}

	/**
	 * Create a block of memory in the superpeer storage.
	 * @param p_storageId
	 *            Local storage id to assign to the newly created block.
	 * @param p_size
	 *            Size of the block to create
	 * @return True if creating successful, false if failed.
	 */
	public boolean superpeerStorageCreate(final int p_storageId, final int p_size) {
		assert p_storageId < Math.pow(2, 31) && p_storageId >= 0;

		boolean check = false;
		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}
		while (true) {
			short responsibleSuperpeer = getResponsibleSuperpeer(m_hashGenerator.hash(p_storageId), check);

			if (-1 != responsibleSuperpeer) {
				SuperpeerStorageCreateRequest request =
						new SuperpeerStorageCreateRequest(responsibleSuperpeer, p_storageId, p_size, false);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {}
					continue;
				}

				SuperpeerStorageCreateResponse response = request.getResponse(SuperpeerStorageCreateResponse.class);
				return response.getStatusCode() == 0;
			}
		}
	}

	/**
	 * Put data into an allocated block in the superpeer storage.
	 * @param p_dataStructure
	 *            Data structure with data to put.
	 * @return True if successful, false otherwise.
	 */
	public boolean superpeerStoragePut(final DataStructure p_dataStructure) {
		if (p_dataStructure.getID() > 0x7FFFFFFF && p_dataStructure.getID() < 0) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot put data structure into superpeer storage, invalid id " + ChunkID
					.toHexString(p_dataStructure.getID()));
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		int storageId = (int) (p_dataStructure.getID() & 0x7FFFFFFF);

		boolean check = false;
		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}

		while (true) {
			short responsibleSuperpeer =
					getResponsibleSuperpeer(m_hashGenerator.hash(storageId), check);

			if (-1 != responsibleSuperpeer) {
				SuperpeerStoragePutRequest request =
						new SuperpeerStoragePutRequest(responsibleSuperpeer, p_dataStructure, false);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {}
					continue;
				}

				SuperpeerStoragePutResponse response = request.getResponse(SuperpeerStoragePutResponse.class);
				return response.getStatusCode() == 0;
			}
		}
	}

	/**
	 * Get data from an allocated block in the superpeer storage.
	 * @param p_id
	 *            Id of the allocated block.
	 * @return Chunk with data from the block or null on error.
	 */
	public Chunk superpeerStorageGet(final int p_id) {
		boolean check = false;
		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}

		while (true) {
			short responsibleSuperpeer =
					getResponsibleSuperpeer(m_hashGenerator.hash(p_id), check);

			if (-1 != responsibleSuperpeer) {
				Chunk chunk = new Chunk((long) p_id);
				SuperpeerStorageGetRequest request =
						new SuperpeerStorageGetRequest(responsibleSuperpeer, chunk);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {}
					continue;
				}

				SuperpeerStorageGetResponse response = request.getResponse(SuperpeerStorageGetResponse.class);
				if (response.getStatusCode() == 0) {
					return chunk;
				} else {
					return null;
				}
			}
		}
	}

	/**
	 * Get data from an allocated block in the superpeer storage.
	 * @param p_dataStructure
	 *            Data structure with set storage id to read the data from the storage into.
	 * @return True if successful, false otherwise.
	 */
	public boolean superpeerStorageGet(final DataStructure p_dataStructure) {
		if (p_dataStructure.getID() > 0x7FFFFFFF && p_dataStructure.getID() < 0) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot get data structure from superpeer storage, invalid id " + ChunkID
					.toHexString(p_dataStructure.getID()));
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		int storageId = (int) (p_dataStructure.getID() & 0x7FFFFFFF);

		boolean check = false;
		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}

		while (true) {
			short responsibleSuperpeer =
					getResponsibleSuperpeer(m_hashGenerator.hash(storageId), check);

			if (-1 != responsibleSuperpeer) {
				SuperpeerStorageGetRequest request =
						new SuperpeerStorageGetRequest(responsibleSuperpeer, p_dataStructure);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {}
					continue;
				}

				SuperpeerStorageGetResponse response = request.getResponse(SuperpeerStorageGetResponse.class);
				return response.getStatusCode() == 0;
			}
		}
	}

	/**
	 * Remove an allocated block in the superpeer storage.
	 * @param p_superpeerStorageId
	 *            Id of the allocated block to remove.
	 */
	public void superpeerStorageRemove(final int p_superpeerStorageId) {
		boolean check = false;
		if (!OverlayHelper.isOverlayStable(m_initialNumberOfSuperpeers, m_superpeers.size())) {
			check = true;
		}

		while (true) {
			short responsibleSuperpeer =
					getResponsibleSuperpeer(m_hashGenerator.hash(p_superpeerStorageId), check);

			if (-1 != responsibleSuperpeer) {
				SuperpeerStorageRemoveMessage message =
						new SuperpeerStorageRemoveMessage(responsibleSuperpeer, p_superpeerStorageId, false);
				if (m_network.sendMessage(message) != NetworkErrorCodes.SUCCESS) {
					// Responsible superpeer is not available, try again (superpeers will be updated
					// automatically by network thread)
					try {
						Thread.sleep(1000);
					} catch (final InterruptedException ignored) {}
					continue;
				}

				return;
			}
		}
	}

	/**
	 * Get the status of the superpeer storage.
	 * @return Status of the superpeer storage.
	 */
	public SuperpeerStorage.Status superpeerStorageGetStatus() {
		SuperpeerStorage.Status[] statusArray = new SuperpeerStorage.Status[m_superpeers.size()];
		for (int i = 0; i < m_superpeers.size(); i++) {
			short superpeer = m_superpeers.get(i);
			SuperpeerStorageStatusRequest request = new SuperpeerStorageStatusRequest(superpeer);
			NetworkErrorCodes err = m_network.sendSync(request);
			if (err != NetworkErrorCodes.SUCCESS) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"Getting superpeer " + NodeID.toHexString(superpeer) + " storage status failed.");
				// #endif /* LOGGER >= ERROR */
				statusArray[i] = null;
			} else {
				statusArray[i] = request.getResponse(SuperpeerStorageStatusResponse.class).getStatus();
			}
		}

		// aggregate status...bad performance =(
		ArrayList<Long> aggregatedStatus = new ArrayList<>();
		for (SuperpeerStorage.Status aStatusArray : statusArray) {
			ArrayList<Long> toMergeArray = aStatusArray.getStatusArray();
			toMergeArray.stream().filter(val -> !aggregatedStatus.contains(val)).forEach(aggregatedStatus::add);
		}

		// and finally...sort
		Collections.sort(aggregatedStatus, (p_o1, p_o2) -> {
			Integer i1 = (int) (p_o1 >> 32);
			Integer i2 = (int) (p_o2 >> 32);

			return i1.compareTo(i2);
		});

		return new SuperpeerStorage.Status(statusArray[0].getMaxNumItems(), statusArray[0].getMaxStorageSizeBytes(),
				aggregatedStatus);
	}

	/**
	 * Joins the superpeer overlay through contactSuperpeer
	 * @param p_contactSuperpeer
	 *            NodeID of a known superpeer
	 * @return whether joining was successful
	 */
	private boolean joinSuperpeerOverlay(final short p_contactSuperpeer) {
		short contactSuperpeer;
		JoinRequest joinRequest;
		JoinResponse joinResponse = null;

		// #if LOGGER == TRACE
		// // // // m_logger.trace(getClass(), "Entering joinSuperpeerOverlay with: p_contactSuperpeer=" + p_contactSuperpeer);
		// #endif /* LOGGER == TRACE */

		contactSuperpeer = p_contactSuperpeer;

		if (p_contactSuperpeer == NodeID.INVALID_ID) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot join superpeer overlay, no bootstrap superpeer available to contact.");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		while (-1 != contactSuperpeer) {
			// #if LOGGER == TRACE
			// // // // m_logger.trace(getClass(),
					// // // // "Contacting " + contactSuperpeer + " to get the responsible superpeer, I am "
							// // // // + NodeID.toHexString(m_nodeID));
			// #endif /* LOGGER == TRACE */

			joinRequest = new JoinRequest(contactSuperpeer, m_nodeID, IS_NOT_SUPERPEER);
			if (m_network.sendSync(joinRequest) != NetworkErrorCodes.SUCCESS) {
				// Contact superpeer is not available, get a new contact superpeer
				contactSuperpeer = m_boot.getNodeIDBootstrap();
				continue;
			}

			joinResponse = joinRequest.getResponse(JoinResponse.class);
			contactSuperpeer = joinResponse.getNewContactSuperpeer();
		}
		assert joinResponse != null;
		m_superpeers = joinResponse.getSuperpeers();
		m_mySuperpeer = joinResponse.getSource();
		OverlayHelper.insertSuperpeer(m_mySuperpeer, m_superpeers);

		// #if LOGGER == TRACE
		// // // // m_logger.trace(getClass(), "Exiting joinSuperpeerOverlay");
		// #endif /* LOGGER == TRACE */

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
		AskAboutSuccessorRequest request;
		AskAboutSuccessorResponse response;

		// #if LOGGER == TRACE
		// // // // m_logger.trace(OverlayHelper.class,
				// // // // "Entering getResponsibleSuperpeer with: p_nodeID=" + NodeID.toHexString(p_nodeID));
		// #endif /* LOGGER == TRACE */

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
			// #if LOGGER >= WARN
			m_logger.warn(OverlayHelper.class, "do not know any superpeer");
			// #endif /* LOGGER >= WARN */
			m_overlayLock.unlock();
		}
		// #if LOGGER == TRACE
		// // // // m_logger.trace(OverlayHelper.class, "Exiting getResponsibleSuperpeer");
		// #endif /* LOGGER == TRACE */

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
		// #if LOGGER == TRACE
		// // // // m_logger.trace(getClass(), "Got Message: SEND_SUPERPEERS_MESSAGE from " + NodeID.toHexString(source));
		// #endif /* LOGGER == TRACE */

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

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST,
				BarrierAllocRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_ALLOC_RESPONSE,
				BarrierAllocResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_FREE_REQUEST,
				BarrierFreeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_FREE_RESPONSE,
				BarrierFreeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_SIGN_ON_REQUEST,
				BarrierSignOnRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_SIGN_ON_RESPONSE,
				BarrierSignOnResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_RELEASE_MESSAGE,
				BarrierReleaseMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_STATUS_REQUEST,
				BarrierGetStatusRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_STATUS_RESPONSE,
				BarrierGetStatusResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST,
				BarrierChangeSizeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_RESPONSE,
				BarrierChangeSizeResponse.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST,
				SuperpeerStorageCreateRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_RESPONSE,
				SuperpeerStorageCreateResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST,
				SuperpeerStorageGetRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_RESPONSE,
				SuperpeerStorageGetResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST,
				SuperpeerStoragePutRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_RESPONSE,
				SuperpeerStoragePutResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE,
				SuperpeerStorageRemoveMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST,
				SuperpeerStorageStatusRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_RESPONSE,
				SuperpeerStorageStatusResponse.class);
	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(SendSuperpeersMessage.class, this);
		m_network.register(NameserviceUpdatePeerCachesMessage.class, this);
	}

}
