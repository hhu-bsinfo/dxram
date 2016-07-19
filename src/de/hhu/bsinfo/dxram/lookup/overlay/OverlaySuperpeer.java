
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.data.Chunk;
import de.hhu.bsinfo.dxram.data.ChunkID;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.lookup.LookupRange;
import de.hhu.bsinfo.dxram.lookup.LookupRangeWithBackupPeers;
import de.hhu.bsinfo.dxram.lookup.event.NodeFailureEvent;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsResponse;
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
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutFailedPeerMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewPredecessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewSuccessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.PingSuperpeerMessage;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.RemoveChunkIDsResponse;
import de.hhu.bsinfo.dxram.lookup.messages.SendBackupsMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SendSuperpeersMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SetRestorerAfterRecoveryMessage;
import de.hhu.bsinfo.dxram.lookup.messages.StartRecoveryMessage;
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
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;
import de.hhu.bsinfo.utils.CRC16;

/**
 * Superpper functionality for overlay
 *
 * @author Kevin Beineke <kevin.beineke@hhu.de> 30.03.16
 */
public class OverlaySuperpeer implements MessageReceiver {

	private static final short ORDER = 10;

	private static final short CLOSED_INTERVAL = 0;
	private static final short UPPER_CLOSED_INTERVAL = 1;
	private static final short OPEN_INTERVAL = 2;
	private static final boolean IS_SUPERPEER = true;
	private static final boolean BACKUP = true;
	private static final short DUMMY = -1;

	// Attributes
	private NetworkComponent m_network;
	private AbstractBootComponent m_boot;
	private EventComponent m_event;
	private LoggerComponent m_logger;

	private short m_nodeID = -1;
	private short m_predecessor = -1;
	private short m_successor = -1;
	private short m_bootstrap = -1;
	private int m_initialNumberOfSuperpeers;
	private ArrayList<Short> m_superpeers;
	private ArrayList<Short> m_peers;

	private LookupTree[] m_nodeTable;
	private ArrayList<Short> m_assignedPeersIncludingBackup;

	private CRC16 m_hashGenerator = new CRC16();
	private NameserviceHashTable m_idTable;

	private BarriersTable m_barriersTable;

	private SuperpeerStabilizationThread m_stabilizationThread;

	private ReentrantLock m_overlayLock;
	private ReentrantLock m_dataLock;
	private ReentrantLock m_mappingLock;
	private ReentrantLock m_failureLock;

	private SuperpeerStorage m_storage;
	private ReadWriteLock m_storageLock = new ReentrantReadWriteLock(false);

	/**
	 * Creates an instance of OverlaySuperpeer
	 *
	 * @param p_nodeID                    the own NodeID
	 * @param p_contactSuperpeer          the superpeer to contact for joining
	 * @param p_initialNumberOfSuperpeers the number of expeced superpeers
	 * @param p_sleepInterval             the ping interval
	 * @param p_maxNumOfBarriers          Max number of barriers
	 * @param p_storageMaxNumEntries      Max number of entries for the superpeer storage (-1 to disable)
	 * @param p_storageMaxSizeBytes       Max size for the superpeer storage in bytes
	 * @param p_boot                      the BootComponent
	 * @param p_logger                    the LoggerComponent
	 * @param p_network                   the NetworkComponent
	 * @param p_event                     the EventComponent
	 */
	public OverlaySuperpeer(final short p_nodeID, final short p_contactSuperpeer, final int p_initialNumberOfSuperpeers,
			final int p_sleepInterval, final int p_maxNumOfBarriers, final int p_storageMaxNumEntries,
			final int p_storageMaxSizeBytes,
			final AbstractBootComponent p_boot, final LoggerComponent p_logger, final NetworkComponent p_network,
			final EventComponent p_event) {
		m_boot = p_boot;
		m_event = p_event;
		m_logger = p_logger;
		m_network = p_network;

		m_nodeID = p_nodeID;
		m_initialNumberOfSuperpeers = p_initialNumberOfSuperpeers;

		registerNetworkMessages();
		registerNetworkMessageListener();

		m_nodeTable = new LookupTree[NodeID.MAX_ID];
		m_assignedPeersIncludingBackup = new ArrayList<>();
		m_idTable = new NameserviceHashTable(1000, 0.9f, m_logger);

		m_barriersTable = new BarriersTable(p_maxNumOfBarriers, m_nodeID);

		m_superpeers = new ArrayList<>();
		m_peers = new ArrayList<>();

		m_overlayLock = new ReentrantLock(false);
		m_dataLock = new ReentrantLock(false);
		m_mappingLock = new ReentrantLock(false);
		m_failureLock = new ReentrantLock(false);

		m_initialNumberOfSuperpeers--;

		m_storage = new SuperpeerStorage(p_storageMaxNumEntries, p_storageMaxSizeBytes);

		createOrJoinSuperpeerOverlay(p_contactSuperpeer, p_sleepInterval);
	}

	/**
	 * Shuts down the stabilization thread
	 */
	public void shutdown() {
		m_stabilizationThread.interrupt();
		m_stabilizationThread.shutdown();
		try {
			m_stabilizationThread.join();
			// #if LOGGER >= INFO
			m_logger.info(getClass(), "Shutdown of StabilizationThread successful.");
			// #endif /* LOGGER >= INFO */
		} catch (final InterruptedException e) {
			// #if LOGGER >= WARN
			m_logger.warn(getClass(), "Could not wait for stabilization thread to finish. Interrupted.");
			// #endif /* LOGGER >= WARN */
		}
	}

	/**
	 * Returns whether this superpeer is last in overlay or not
	 *
	 * @return whether this superpeer is last in overlay or not
	 */
	public boolean isLastSuperpeer() {
		boolean ret = true;
		short superpeer;
		int i = 0;

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

		return ret;
	}

	/**
	 * Returns current predecessor
	 *
	 * @return the predecessor
	 */
	protected short getPredecessor() {
		return m_predecessor;
	}

	/**
	 * Sets the predecessor for the current superpeer
	 *
	 * @param p_nodeID NodeID of the predecessor
	 * @note assumes m_overlayLock has been locked
	 */
	protected void setPredecessor(final short p_nodeID) {
		m_predecessor = p_nodeID;
		if (m_predecessor != m_successor) {
			OverlayHelper.insertSuperpeer(m_predecessor, m_superpeers);
		}
	}

	/**
	 * Returns current successor
	 *
	 * @return the sucessor
	 */
	protected short getSuccessor() {
		return m_successor;
	}

	/**
	 * Sets the successor for the current superpeer
	 *
	 * @param p_nodeID NodeID of the successor
	 * @note assumes m_overlayLock has been locked
	 */
	protected void setSuccessor(final short p_nodeID) {
		m_successor = p_nodeID;
		if (-1 != m_successor && m_nodeID != m_successor) {
			OverlayHelper.insertSuperpeer(m_successor, m_superpeers);
		}
	}

	/**
	 * Returns all peers
	 *
	 * @return all peers
	 */
	protected ArrayList<Short> getPeers() {
		return m_peers;
	}

	/**
	 * Determines all peers that are in the responsible area
	 *
	 * @param p_oldSuperpeer     the old superpeer
	 * @param p_currentSuperpeer the new superpeer
	 * @return all peers in responsible area
	 */
	ArrayList<Short> getPeersInResponsibleArea(final short p_oldSuperpeer, final short p_currentSuperpeer) {
		short currentPeer;
		int index;
		int startIndex;
		ArrayList<Short> peers;

		peers = new ArrayList<Short>();
		m_dataLock.lock();
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, p_oldSuperpeer);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_assignedPeersIncludingBackup.get(index++);
			while (OverlayHelper.isNodeInRange(currentPeer, p_oldSuperpeer, p_currentSuperpeer, OPEN_INTERVAL)) {
				peers.add(Collections.binarySearch(peers, currentPeer) * -1 - 1, currentPeer);
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_assignedPeersIncludingBackup.get(index++);
			}
		}
		m_dataLock.unlock();

		return peers;
	}

	/**
	 * Removes given peer from all backup ranges as backup peer
	 *
	 * @param p_failedPeer the failed peer
	 */
	void removeFailedPeer(final short p_failedPeer) {
		LookupTree tree;
		Iterator<Short> iter;

		m_dataLock.lock();
		// Remove failedPeer from all backup peer lists
		iter = m_assignedPeersIncludingBackup.iterator();
		while (iter.hasNext()) {
			getCIDTree(iter.next()).removeBackupPeer(p_failedPeer, DUMMY);
		}

		tree = getCIDTree(p_failedPeer);
		if (null != tree) {
			tree.setStatus(false);
		}
		m_dataLock.unlock();
	}

	/**
	 * Compares given peer list with local list and returns all missing backup data
	 *
	 * @param p_peers all peers the requesting superpeer stores backups for
	 * @param p_trees an empty ArrayList to put missing LookupTrees in
	 * @return the backup data of missing peers in given peer list
	 */
	byte[] compareAndReturnBackups(final ArrayList<Short> p_peers, final ArrayList<LookupTree> p_trees) {
		int index;
		int startIndex;
		short currentPeer;
		short lowerBound;
		byte[] allMappings = null;

		m_dataLock.lock();
		lowerBound = m_predecessor;
		// Compare m_nodeList with given list, add missing entries to "trees"
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, lowerBound);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_assignedPeersIncludingBackup.get(index++);
			while (OverlayHelper.isNodeInRange(currentPeer, lowerBound, m_nodeID, OPEN_INTERVAL)) {
				if (0 > Collections.binarySearch(p_peers, currentPeer)) {
					p_trees.add(getCIDTree(currentPeer));
					// #if LOGGER == TRACE
					m_logger.trace(getClass(), "Spreading meta-data of " + NodeID.toHexString(currentPeer)
							+ " to " + NodeID.toHexString(m_successor));
					// #endif /* LOGGER == TRACE */
				}
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_assignedPeersIncludingBackup.get(index++);
			}

			m_mappingLock.lock();
			allMappings = m_idTable.toArray(lowerBound, m_nodeID, false, UPPER_CLOSED_INTERVAL, m_hashGenerator);
			m_mappingLock.unlock();
		}
		m_dataLock.unlock();

		return allMappings;
	}

	/**
	 * Stores given backups
	 *
	 * @param p_trees    the new LookupTrees
	 * @param p_mappings the new mappings
	 */
	void storeIncomingBackups(final ArrayList<LookupTree> p_trees, final byte[] p_mappings) {
		LookupTree tree;

		m_dataLock.lock();
		for (int i = 0; i < p_trees.size(); i++) {
			tree = p_trees.get(i);
			addCIDTree(tree.getCreator(), tree);
		}
		m_dataLock.unlock();

		m_mappingLock.lock();
		m_idTable.putAll(p_mappings);
		m_mappingLock.unlock();
	}

	/**
	 * Deletes all CIDTrees that are not in the responsible area
	 *
	 * @param p_responsibleArea the responsible area
	 * @note assumes m_overlayLock has been locked
	 * @note is called periodically
	 */
	void deleteUnnecessaryBackups(final short[] p_responsibleArea) {
		short currentPeer;
		int index;

		m_dataLock.lock();
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, p_responsibleArea[1]);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
			}
			currentPeer = m_assignedPeersIncludingBackup.get(index);
			while (OverlayHelper.isNodeInRange(currentPeer, p_responsibleArea[1], p_responsibleArea[0], OPEN_INTERVAL)
					&& p_responsibleArea[0] != p_responsibleArea[1]) {
				deleteCIDTree(currentPeer);
				m_idTable.remove(currentPeer);

				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
				if (0 == m_assignedPeersIncludingBackup.size()) {
					break;
				}
				currentPeer = m_assignedPeersIncludingBackup.get(index);
			}
		}
		m_dataLock.unlock();
	}

	/**
	 * Takes over failed superpeers peers and CIDTrees
	 *
	 * @param p_nodeID the NodeID
	 */
	void takeOverPeersAndCIDTrees(final short p_nodeID) {
		short predecessor;
		short firstPeer;
		short currentPeer;
		int index;
		int startIndex;

		m_overlayLock.lock();
		if (m_superpeers.isEmpty()) {
			firstPeer = (short) (m_nodeID + 1);
		} else {
			index = Collections.binarySearch(m_superpeers, p_nodeID);
			if (0 > index) {
				index = index * -1 - 1;
			}
			if (0 == index) {
				predecessor = m_superpeers.get(m_superpeers.size() - 1);
			} else {
				predecessor = m_superpeers.get(index - 1);
			}
			if (predecessor == p_nodeID) {
				firstPeer = (short) (m_nodeID + 1);
			} else {
				firstPeer = predecessor;
			}
		}
		m_overlayLock.unlock();

		m_dataLock.lock();
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, firstPeer);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_assignedPeersIncludingBackup.get(index++);
			while (OverlayHelper.isNodeInRange(currentPeer, firstPeer, p_nodeID, CLOSED_INTERVAL)) {
				if (getCIDTree(currentPeer).getStatus()) {
					if (0 > Collections.binarySearch(m_peers, currentPeer)
							&& 0 > Collections.binarySearch(m_superpeers, currentPeer)) {
						// #if LOGGER >= INFO
						m_logger.info(getClass(), "** Taking over " + NodeID.toHexString(currentPeer));
						// #endif /* LOGGER >= INFO */
						m_overlayLock.lock();
						OverlayHelper.insertPeer(currentPeer, m_peers);
						m_overlayLock.unlock();
					}
				}
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_assignedPeersIncludingBackup.get(index++);
			}
		}
		m_dataLock.unlock();
	}

	/**
	 * Handles a node failure
	 *
	 * @param p_failedNode the failed nodes NodeID
	 */
	void failureHandling(final short p_failedNode) {
		short[] responsibleArea;
		short[] backupSuperpeers;
		short superpeer;
		int i = 0;
		boolean existsInZooKeeper = false;
		Iterator<Short> iter;
		ArrayList<long[]> backupRanges;
		LookupTree tree;

		boolean finished = false;

		if (m_failureLock.tryLock()) {

			m_overlayLock.lock();

			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "********** ********** Node Failure ********** **********");
			// #endif /* LOGGER >= ERROR */

			// Check if failed node is a superpeer
			if (0 <= Collections.binarySearch(m_superpeers, p_failedNode)) {
				m_overlayLock.unlock();

				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Failed node was a superpeer, NodeID: " + NodeID.toHexString(p_failedNode));
				// #endif /* LOGGER >= ERROR */

				// notify others about failure
				m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_failedNode, NodeRole.SUPERPEER));

				// Determine new bootstrap if failed node is current one
				if (p_failedNode == m_bootstrap) {
					determineNewBootstrap();
					// #if LOGGER >= INFO
					m_logger.info(getClass(), "Failed node " + NodeID.toHexString(p_failedNode)
							+ " was bootstrap. New bootstrap is " + NodeID.toHexString(m_bootstrap));
					// #endif /* LOGGER >= INFO */
				}
				// Take over failed nodes peers and CIDTrees if it is this nodes predecessor
				if (p_failedNode == m_predecessor) {
					// #if LOGGER >= INFO
					m_logger.info(getClass(), "Failed node " + NodeID.toHexString(p_failedNode)
							+ " was my predecessor -> taking over all peers and data");
					// #endif /* LOGGER >= INFO */
					takeOverPeersAndCIDTrees(m_predecessor);
				}
				// Send failed nodes CIDTrees to this nodes successor if it is the first node in responsible area
				m_overlayLock.lock();
				responsibleArea = OverlayHelper.getResponsibleArea(m_nodeID, m_predecessor, m_superpeers);
				m_overlayLock.unlock();
				if (3 < m_superpeers.size()
						&& OverlayHelper.getResponsibleSuperpeer((short) (responsibleArea[0] + 1), m_superpeers,
						m_overlayLock, m_logger) == p_failedNode) {
					// #if LOGGER >= INFO
					m_logger.info(getClass(), "Failed node " + NodeID.toHexString(p_failedNode)
							+ " was in my responsible area -> spreading his data");
					// #endif /* LOGGER >= INFO */
					spreadDataOfFailedSuperpeer(p_failedNode, responsibleArea);
				}
				// Send this nodes CIDTrees to new backup node that replaces the failed node
				m_overlayLock.lock();
				backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
				m_overlayLock.unlock();
				if (3 < m_superpeers.size() && OverlayHelper.isNodeInRange(p_failedNode, backupSuperpeers[0],
						backupSuperpeers[2], CLOSED_INTERVAL)) {
					// #if LOGGER >= INFO
					m_logger.info(getClass(), "Failed node " + NodeID.toHexString(p_failedNode)
							+ " was one of my backup nodes -> spreading my data");
					// #endif /* LOGGER >= INFO */
					spreadBackupsOfThisSuperpeer(backupSuperpeers);
				}
				// Remove superpeer
				m_overlayLock.lock();
				final int index = OverlayHelper.removeSuperpeer(p_failedNode, m_superpeers);
				if (0 <= index) {
					// Set new predecessor/successor if failed superpeer was pre-/succeeding
					if (p_failedNode == m_successor) {
						if (0 != m_superpeers.size()) {
							if (index < m_superpeers.size()) {
								m_successor = m_superpeers.get(index);
							} else {
								m_successor = m_superpeers.get(0);
							}
						} else {
							m_successor = (short) -1;
						}
					}

					if (p_failedNode == m_predecessor) {
						if (0 != m_superpeers.size()) {
							if (0 < index) {
								m_predecessor = m_superpeers.get(index - 1);
							} else {
								m_predecessor = m_superpeers.get(m_superpeers.size() - 1);
							}
						} else {
							m_predecessor = (short) -1;
						}
					}
				}
				m_overlayLock.unlock();

				m_boot.reportNodeFailure(p_failedNode, true);

				m_failureLock.unlock();
			} else if (0 <= Collections.binarySearch(m_peers, p_failedNode)) {
				m_overlayLock.unlock();
				existsInZooKeeper = m_boot.nodeAvailable(p_failedNode);

				if (!existsInZooKeeper) {
					// Failed node was a terminal
					// #if LOGGER >= ERROR
					m_logger.error(getClass(),
							"Failed node was a terminal, NodeID: " + NodeID.toHexString(p_failedNode));
					// #endif /* LOGGER >= ERROR */

					// Remove peer
					m_overlayLock.lock();
					OverlayHelper.removePeer(p_failedNode, m_peers);
					m_overlayLock.unlock();

					// #if LOGGER >= INFO
					m_logger.info(getClass(),
							"Failed node " + NodeID.toHexString(p_failedNode) + ": no further actions required");
					// #endif /* LOGGER >= INFO */

					// notify others about failure
					m_event.fireEvent(
							new NodeFailureEvent(getClass().getSimpleName(), p_failedNode, NodeRole.TERMINAL));

					// make sure to remove from boot as well
					m_boot.reportNodeFailure(p_failedNode, false);
				} else {
					// Failed node was a peer
					// #if LOGGER >= ERROR
					m_logger.error(getClass(), "Failed node was a peer, NodeID: " + NodeID.toHexString(p_failedNode));
					// #endif /* LOGGER >= ERROR */

					// notify others about failure
					m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_failedNode, NodeRole.PEER));

					// Remove peer in meta-data (and replace with new backup node; DUMMY element currently)
					// #if LOGGER >= INFO
					m_logger.info(getClass(), "Removing " + NodeID.toHexString(p_failedNode) + " from local meta-data");
					// #endif /* LOGGER >= INFO */
					m_dataLock.lock();
					iter = m_assignedPeersIncludingBackup.iterator();
					while (iter.hasNext()) {
						tree = getCIDTree(iter.next());
						if (tree != null) {
							tree.removeBackupPeer(p_failedNode, DUMMY);
						}
					}
					tree = getCIDTree(p_failedNode);
					if (tree != null) {
						tree.setStatus(false);
					}
					m_dataLock.unlock();
					while (true) {
						m_overlayLock.lock();
						if (i < m_superpeers.size()) {
							superpeer = m_superpeers.get(i++);
							m_overlayLock.unlock();
						} else {
							m_overlayLock.unlock();
							break;
						}
						// Inform superpeer about failed peer to initialize deletion
						// #if LOGGER >= INFO
						m_logger.info(getClass(), "Informing " + NodeID.toHexString(superpeer)
								+ " to remove " + NodeID.toHexString(p_failedNode) + " from meta-data");
						// #endif /* LOGGER >= INFO */
						if (m_network.sendMessage(
								new NotifyAboutFailedPeerMessage(superpeer,
										p_failedNode)) != NetworkErrorCodes.SUCCESS) {
							// Superpeer is not available anymore, remove from superpeer array and continue
							// #if LOGGER >= ERROR
							m_logger.error(getClass(), "superpeer failed, too");
							// #endif /* LOGGER >= ERROR */
							m_failureLock.unlock();
							failureHandling(superpeer);
							m_failureLock.lock();
						}
					}

					// Start recovery
					// #if LOGGER >= INFO
					m_logger.info(getClass(), "Starting recovery for failed node " + NodeID.toHexString(p_failedNode));
					// #endif /* LOGGER >= INFO */
					while (!finished) {
						finished = true;
						m_dataLock.lock();
						tree = getCIDTree(p_failedNode);
						// no tree available -> no chunks were created
						if (tree == null) {
							backupRanges = null;
						} else {
							backupRanges = tree.getAllBackupRanges();
						}

						m_dataLock.unlock();
						if (backupRanges != null) {
							for (i = 0; i < backupRanges.size(); i++) {
								for (int j = 0; j < 3; j++) {
									// backupRanges.get(i)[0];
									// (short) (backupRanges.get(i)[1] >> j * 16);
									// Inform backupPeer to recover all chunks between (i * 1000) and ((i + 1) * 1000 -
									// 1)

									// #if LOGGER >= INFO
									m_logger.info(getClass(), "Starting recovery (not implemented, no execution)");
									// #endif /* LOGGER >= INFO */
									// TODO
									/*
									 * try {
									 * new StartRecoveryMessage(backupPeer, p_failedNode, i * 1000).send(m_network);
									 * } catch (final NetworkException e) {
									 * // Backup peer is not available anymore, try next one
									 * continue;
									 * }
									 */
								}
							}
						}
					}

					// Remove peer
					m_overlayLock.lock();
					OverlayHelper.removePeer(p_failedNode, m_peers);
					m_overlayLock.unlock();
					m_boot.reportNodeFailure(p_failedNode, false);

					m_failureLock.unlock();

					// #if LOGGER >= INFO
					m_logger.info(getClass(),
							"Recovery of failed node " + NodeID.toHexString(p_failedNode) + " complete.");
					// #endif /* LOGGER >= INFO */
				}
			}
		}
	}

	/**
	 * Joins the superpeer overlay through contactSuperpeer
	 *
	 * @param p_contactSuperpeer NodeID of a known superpeer
	 * @param p_sleepInterval    the ping interval
	 * @return whether the joining was successful
	 */
	private boolean createOrJoinSuperpeerOverlay(final short p_contactSuperpeer, final int p_sleepInterval) {
		short contactSuperpeer;
		JoinRequest joinRequest;
		JoinResponse joinResponse = null;
		ArrayList<LookupTree> trees;
		LookupTree tree;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Entering createOrJoinSuperpeerOverlay with: p_contactSuperpeer="
				+ NodeID.toHexString(p_contactSuperpeer));
		// #endif /* LOGGER == TRACE */

		contactSuperpeer = p_contactSuperpeer;

		if (p_contactSuperpeer == NodeID.INVALID_ID) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Cannot join superpeer overlay, no bootstrap superpeer available to contact.");
			// #endif /* LOGGER >= ERROR */
			return false;
		}

		if (m_nodeID == contactSuperpeer) {
			if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
				// #if LOGGER == TRACE
				m_logger.trace(getClass(), "Setting up new ring, I am " + NodeID.toHexString(m_nodeID));
				// #endif /* LOGGER == TRACE */
				setSuccessor(m_nodeID);
			} else {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "Bootstrap has to be a superpeer, exiting now.");
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		} else {
			while (-1 != contactSuperpeer) {
				// #if LOGGER == TRACE
				m_logger.trace(getClass(), "Contacting " + NodeID.toHexString(contactSuperpeer)
						+ " to join the ring, I am " + NodeID.toHexString(m_nodeID));
				// #endif /* LOGGER == TRACE */

				joinRequest = new JoinRequest(contactSuperpeer, m_nodeID, IS_SUPERPEER);
				if (m_network.sendSync(joinRequest) != NetworkErrorCodes.SUCCESS) {
					// Contact superpeer is not available, get a new contact superpeer
					contactSuperpeer = m_boot.getNodeIDBootstrap();
					continue;
				}

				joinResponse = joinRequest.getResponse(JoinResponse.class);
				contactSuperpeer = joinResponse.getNewContactSuperpeer();
			}

			m_superpeers = joinResponse.getSuperpeers();

			m_peers = joinResponse.getPeers();

			trees = joinResponse.getCIDTrees();
			for (LookupTree tree1 : trees) {
				tree = tree1;
				addCIDTree(tree.getCreator(), tree1);
			}
			m_idTable.putAll(joinResponse.getMappings());

			setSuccessor(joinResponse.getSuccessor());
			setPredecessor(joinResponse.getPredecessor());
		}

		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Starting stabilization thread");
		// #endif /* LOGGER == TRACE */
		m_stabilizationThread =
				new SuperpeerStabilizationThread(this, m_nodeID, m_overlayLock, m_initialNumberOfSuperpeers,
						m_superpeers, p_sleepInterval, m_logger, m_network);
		m_stabilizationThread.setName(
				SuperpeerStabilizationThread.class.getSimpleName() + " for " + LookupComponent.class.getSimpleName());
		m_stabilizationThread.setDaemon(true);
		m_stabilizationThread.start();

		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Exiting createOrJoinSuperpeerOverlay");
		// #endif /* LOGGER == TRACE */

		return true;
	}

	/**
	 * Determines a new bootstrap
	 */
	private void determineNewBootstrap() {
		// replace bootstrap
		m_bootstrap = m_boot.setBootstrapPeer(m_nodeID);

		if (m_bootstrap != m_nodeID) {
			if (m_network.sendMessage(
					new PingSuperpeerMessage(m_bootstrap)) != NetworkErrorCodes.SUCCESS) {
				// New bootstrap is not available, start failure handling to
				// remove bootstrap from superpeer array and to determine a new bootstrap
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "new bootstrap failed, too");
				// #endif /* LOGGER >= ERROR */
				m_failureLock.unlock();
				failureHandling(m_bootstrap);
				m_failureLock.lock();
			}
		}
	}

	/**
	 * Returns the requested CIDTree
	 *
	 * @param p_nodeID NodeID for that the CIDTree is requested
	 * @return the CIDTree for given NodeID
	 * @note assumes m_dataLock has been locked
	 */
	private LookupTree getCIDTree(final short p_nodeID) {
		return m_nodeTable[p_nodeID & 0xFFFF];
	}

	/**
	 * Adds the given CIDTree to NIDTable
	 *
	 * @param p_nodeID the NodeID
	 * @param p_tree   the CIDTree to add
	 * @note assumes m_dataLock has been locked
	 */
	private void addCIDTree(final short p_nodeID, final LookupTree p_tree) {
		int index;
		index = Collections.binarySearch(m_assignedPeersIncludingBackup, p_nodeID);
		if (0 > index) {
			index = index * -1 - 1;
			m_assignedPeersIncludingBackup.add(index, p_nodeID);
		}
		m_nodeTable[p_nodeID & 0xFFFF] = p_tree;
	}

	/**
	 * Deletes the given CIDTree
	 *
	 * @param p_nodeID the NodeID
	 * @note assumes m_dataLock has been locked
	 */
	private void deleteCIDTree(final short p_nodeID) {
		int index;
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, p_nodeID);
			if (0 <= index) {
				m_assignedPeersIncludingBackup.remove(index);
				m_nodeTable[p_nodeID & 0xFFFF] = null;
			}
		}
	}

	/**
	 * Spread data of failed superpeer
	 *
	 * @param p_nodeID          the NodeID
	 * @param p_responsibleArea the responsible area
	 */
	private void spreadDataOfFailedSuperpeer(final short p_nodeID, final short[] p_responsibleArea) {
		short currentPeer;
		int index;
		int startIndex;
		byte[] allMappings = null;
		ArrayList<LookupTree> trees;

		m_dataLock.lock();
		trees = new ArrayList<LookupTree>();
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, p_responsibleArea[0]);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_assignedPeersIncludingBackup.get(index++);
			while (OverlayHelper.isNodeInRange(currentPeer, p_responsibleArea[0], p_nodeID, OPEN_INTERVAL)) {
				trees.add(getCIDTree(currentPeer));

				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_assignedPeersIncludingBackup.get(index++);
			}

			m_mappingLock.lock();
			allMappings =
					m_idTable.toArray(p_responsibleArea[0], p_nodeID, false, UPPER_CLOSED_INTERVAL, m_hashGenerator);
			m_mappingLock.unlock();
		}
		m_dataLock.unlock();

		while (!m_superpeers.isEmpty()) {
			// #if LOGGER >= INFO
			m_logger.info(getClass(), "Spreading failed superpeers meta-data to " + NodeID.toHexString(m_successor));
			// #endif /* LOGGER >= INFO */
			if (m_network.sendMessage(
					new SendBackupsMessage(m_successor, allMappings, trees)) != NetworkErrorCodes.SUCCESS) {
				// Successor is not available anymore, remove from superpeer array and try next superpeer
				// #if LOGGER >= ERROR
				m_logger.error(getClass(), "successor failed, too");
				// #endif /* LOGGER >= ERROR */
				m_failureLock.unlock();
				failureHandling(m_successor);
				m_failureLock.lock();
				continue;
			}
			break;
		}
	}

	/**
	 * Spread backups of failed superpeer
	 *
	 * @param p_backupSuperpeers the current backup superpeers
	 */
	private void spreadBackupsOfThisSuperpeer(final short[] p_backupSuperpeers) {
		short currentPeer;
		short newBackupSuperpeer;
		short lowerBound;
		int index;
		int startIndex;
		boolean dataToTransmit = false;
		boolean superpeerToSendData = false;
		byte[] allMappings = null;
		ArrayList<LookupTree> trees;
		String str = "Spreaded data of ";

		m_dataLock.lock();
		trees = new ArrayList<LookupTree>();
		lowerBound = m_predecessor;
		if (0 != m_assignedPeersIncludingBackup.size()) {
			index = Collections.binarySearch(m_assignedPeersIncludingBackup, lowerBound);
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
			}
			startIndex = index;
			currentPeer = m_assignedPeersIncludingBackup.get(index++);
			while (OverlayHelper.isNodeInRange(currentPeer, lowerBound, m_nodeID, OPEN_INTERVAL)) {
				dataToTransmit = true;
				str += NodeID.toHexString(currentPeer);

				trees.add(getCIDTree(currentPeer));

				if (index == m_assignedPeersIncludingBackup.size()) {
					index = 0;
				}
				if (index == startIndex) {
					break;
				}
				currentPeer = m_assignedPeersIncludingBackup.get(index++);
			}

			m_mappingLock.lock();
			allMappings = m_idTable.toArray(lowerBound, m_nodeID, false, UPPER_CLOSED_INTERVAL, m_hashGenerator);
			m_mappingLock.unlock();
		}
		m_dataLock.unlock();

		while (!m_superpeers.isEmpty()) {
			m_overlayLock.lock();
			index = (short) Collections.binarySearch(m_superpeers, (short) (p_backupSuperpeers[2] + 1));
			if (0 > index) {
				index = index * -1 - 1;
				if (index == m_superpeers.size()) {
					index = 0;
				}
			}
			newBackupSuperpeer = m_superpeers.get(index);
			m_overlayLock.unlock();

			superpeerToSendData = true;
			str += " to " + NodeID.toHexString(newBackupSuperpeer);

			if (m_network.sendMessage(
					new SendBackupsMessage(newBackupSuperpeer, allMappings, trees)) != NetworkErrorCodes.SUCCESS) {
				// Superpeer is not available anymore, remove from superpeer array and try next superpeer
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"new backup superpeer (" + NodeID.toHexString(newBackupSuperpeer) + ") failed, too");
				// #endif /* LOGGER >= ERROR */
				m_failureLock.unlock();
				failureHandling(newBackupSuperpeer);
				m_failureLock.lock();
				continue;
			}
			break;
		}
		// #if LOGGER >= INFO
		if (dataToTransmit && superpeerToSendData) {
			m_logger.info(getClass(), str);
		} else {
			m_logger.info(getClass(), "No need to spread data");
		}
		// #endif /* LOGGER >= INFO */
	}

	/**
	 * Handles an incoming JoinRequest
	 *
	 * @param p_joinRequest the JoinRequest
	 */
	private void incomingJoinRequest(final JoinRequest p_joinRequest) {
		short joiningNode;
		short currentPeer;
		int index;
		int startIndex;
		Iterator<Short> iter;
		ArrayList<Short> peers;
		ArrayList<LookupTree> trees;

		byte[] mappings;
		short joiningNodesPredecessor;
		short superpeer;
		short[] responsibleArea;

		boolean newNodeisSuperpeer;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Got request: JOIN_REQUEST from " + NodeID.toHexString(p_joinRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		joiningNode = p_joinRequest.getNewNode();
		newNodeisSuperpeer = p_joinRequest.nodeIsSuperpeer();

		if (m_superpeers.isEmpty()
				|| OverlayHelper.isNodeInRange(joiningNode, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
			if (newNodeisSuperpeer) {
				m_overlayLock.lock();
				// Send the joining node not only the successor, but the predecessor, superpeers
				// and all relevant CIDTrees
				if (m_superpeers.isEmpty()) {
					joiningNodesPredecessor = m_nodeID;
				} else {
					joiningNodesPredecessor = m_predecessor;
				}

				iter = m_peers.iterator();
				peers = new ArrayList<>();
				while (iter.hasNext()) {
					currentPeer = iter.next();
					if (OverlayHelper.isNodeInRange(currentPeer, joiningNodesPredecessor, joiningNode, OPEN_INTERVAL)) {
						peers.add(currentPeer);
					}
				}

				m_dataLock.lock();
				trees = new ArrayList<>();
				responsibleArea = OverlayHelper.getResponsibleArea(joiningNode, m_predecessor, m_superpeers);
				if (0 != m_assignedPeersIncludingBackup.size()) {
					index = Collections.binarySearch(m_assignedPeersIncludingBackup, responsibleArea[0]);
					if (0 > index) {
						index = index * -1 - 1;
						if (index == m_assignedPeersIncludingBackup.size()) {
							index = 0;
						}
					}
					startIndex = index;
					currentPeer = m_assignedPeersIncludingBackup.get(index++);
					while (OverlayHelper.isNodeInRange(currentPeer, responsibleArea[0], responsibleArea[1],
							OPEN_INTERVAL)) {
						trees.add(getCIDTree(currentPeer));
						if (index == m_assignedPeersIncludingBackup.size()) {
							index = 0;
						}
						if (index == startIndex) {
							break;
						}
						currentPeer = m_assignedPeersIncludingBackup.get(index++);
					}
				}
				m_dataLock.unlock();

				m_mappingLock.lock();
				mappings = m_idTable.toArray(responsibleArea[0], responsibleArea[1], m_superpeers.isEmpty(),
						UPPER_CLOSED_INTERVAL, m_hashGenerator);
				m_mappingLock.unlock();

				if (m_network.sendMessage(new JoinResponse(p_joinRequest, (short) -1, joiningNodesPredecessor, m_nodeID,
						mappings, m_superpeers, peers, trees)) != NetworkErrorCodes.SUCCESS) {
					// Joining node is not available anymore -> ignore request and return directly
					m_overlayLock.unlock();
					return;
				}

				for (Short peer : peers) {
					OverlayHelper.removePeer(peer, m_peers);
				}

				// Notify predecessor about the joining node
				if (m_superpeers.isEmpty()) {
					setSuccessor(joiningNode);
					setPredecessor(joiningNode);
					m_overlayLock.unlock();
				} else {
					setPredecessor(joiningNode);
					m_overlayLock.unlock();

					if (m_network.sendMessage(new NotifyAboutNewSuccessorMessage(joiningNodesPredecessor,
							m_predecessor)) != NetworkErrorCodes.SUCCESS) {
						// Old predecessor is not available anymore, ignore it
					}
				}
			} else {
				m_overlayLock.lock();
				OverlayHelper.insertPeer(joiningNode, m_peers);
				if (m_network.sendMessage(
						new JoinResponse(p_joinRequest, (short) -1, (short) -1, (short) -1, null, m_superpeers, null,
								null)) != NetworkErrorCodes.SUCCESS) {
					// Joining node is not available anymore, ignore request
				}

				m_overlayLock.unlock();
			}
		} else {
			superpeer = OverlayHelper.getResponsibleSuperpeer(joiningNode, m_superpeers, m_overlayLock, m_logger);
			if (m_network.sendMessage(
					new JoinResponse(p_joinRequest, superpeer, (short) -1, (short) -1, null, null, null,
							null)) != NetworkErrorCodes.SUCCESS) {
				// Joining node is not available anymore, ignore request
			}
		}
	}

	/**
	 * Handles an incoming GetLookupRangeRequest
	 *
	 * @param p_getLookupRangeRequest the GetLookupRangeRequest
	 */
	private void incomingGetLookupRangeRequest(final GetLookupRangeRequest p_getLookupRangeRequest) {
		long chunkID;
		LookupRange result = null;
		LookupTree tree;

		chunkID = p_getLookupRangeRequest.getChunkID();
		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"Got request: GET_LOOKUP_RANGE_REQUEST " + NodeID.toHexString(p_getLookupRangeRequest.getSource())
						+ " chunkID: " + ChunkID.toHexString(chunkID));
		// #endif /* LOGGER == TRACE */

		m_dataLock.lock();
		tree = getCIDTree(ChunkID.getCreatorID(chunkID));
		if (null != tree) {
			result = tree.getMetadata(chunkID);
		}
		m_dataLock.unlock();

		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"GET_LOOKUP_RANGE_REQUEST " + NodeID.toHexString(p_getLookupRangeRequest.getSource()) + " chunkID "
						+ ChunkID.toHexString(chunkID) + " reply location: " + result);
		// #endif /* LOGGER == TRACE */

		if (m_network.sendMessage(
				new GetLookupRangeResponse(p_getLookupRangeRequest, result)) != NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming RemoveChunkIDsRequest
	 *
	 * @param p_removeChunkIDsRequest the RemoveChunkIDsRequest
	 */
	private void incomingRemoveChunkIDsRequest(final RemoveChunkIDsRequest p_removeChunkIDsRequest) {
		long[] chunkIDs;
		short creator;
		short[] backupSuperpeers;
		boolean isBackup;
		LookupTree tree;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"Got Message: REMOVE_CHUNKIDS_REQUEST from " + NodeID.toHexString(p_removeChunkIDsRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		chunkIDs = p_removeChunkIDsRequest.getChunkIDs();
		isBackup = p_removeChunkIDsRequest.isBackup();

		for (long chunkID : chunkIDs) {
			creator = ChunkID.getCreatorID(chunkID);
			if (m_superpeers.isEmpty()
					|| OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
				m_dataLock.lock();
				tree = getCIDTree(creator);
				if (null == tree) {
					m_dataLock.unlock();
					// #if LOGGER >= ERROR
					m_logger.error(getClass(),
							"CIDTree range not initialized on responsible superpeer " + NodeID.toHexString(m_nodeID));
					// #endif /* LOGGER >= ERROR */
					if (m_network.sendMessage(
							new RemoveChunkIDsResponse(p_removeChunkIDsRequest,
									new short[] {-1})) != NetworkErrorCodes.SUCCESS) {
						// Requesting peer is not available anymore, ignore it
					}
				} else {
					tree.removeObject(chunkID);
					m_dataLock.unlock();

					m_overlayLock.lock();
					backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
					m_overlayLock.unlock();
					if (m_network.sendMessage(
							new RemoveChunkIDsResponse(p_removeChunkIDsRequest,
									backupSuperpeers)) != NetworkErrorCodes.SUCCESS) {
						// Requesting peer is not available anymore, ignore it
					}
				}
			} else if (isBackup) {
				m_dataLock.lock();
				tree = getCIDTree(creator);
				if (null == tree) {
					// #if LOGGER >= WARN
					m_logger.warn(getClass(),
							"CIDTree range not initialized on backup superpeer " + NodeID.toHexString(m_nodeID));
					// #endif /* LOGGER >= WARN */
				} else {
					tree.removeObject(chunkID);
				}
				m_dataLock.unlock();
				if (m_network.sendMessage(
						new RemoveChunkIDsResponse(p_removeChunkIDsRequest, null)) != NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			} else {
				// Not responsible for requesting peer
				if (m_network.sendMessage(
						new RemoveChunkIDsResponse(p_removeChunkIDsRequest, null)) != NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		}
	}

	/**
	 * Handles an incoming InsertIDRequest
	 *
	 * @param p_insertIDRequest the InsertIDRequest
	 */
	private void incomingInsertNameserviceEntriesRequest(final InsertNameserviceEntriesRequest p_insertIDRequest) {
		int id;
		short[] backupSuperpeers;

		id = p_insertIDRequest.getID();
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Got request: INSERT_ID_REQUEST from "
				+ NodeID.toHexString(p_insertIDRequest.getSource()) + ", id " + id);
		// #endif /* LOGGER == TRACE */

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(m_hashGenerator.hash(id), m_predecessor, m_nodeID,
				UPPER_CLOSED_INTERVAL)) {
			m_mappingLock.lock();
			m_idTable.put(id, p_insertIDRequest.getChunkID());
			m_mappingLock.unlock();

			m_overlayLock.lock();
			backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
			m_overlayLock.unlock();
			if (m_network.sendMessage(
					new InsertNameserviceEntriesResponse(p_insertIDRequest,
							backupSuperpeers)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}

			ArrayList<Short> peers = getPeers();
			// notify peers about this to update caches
			for (short peer : peers) {
				NameserviceUpdatePeerCachesMessage message =
						new NameserviceUpdatePeerCachesMessage(peer, id, p_insertIDRequest.getChunkID());
				if (m_network.sendMessage(message) != NetworkErrorCodes.SUCCESS) {
					// peer is not available anymore, ignore it
				}
			}
		} else if (p_insertIDRequest.isBackup()) {
			m_mappingLock.lock();
			m_idTable.put(id, p_insertIDRequest.getChunkID());
			m_mappingLock.unlock();

			if (m_network.sendMessage(
					new InsertNameserviceEntriesResponse(p_insertIDRequest, null)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for that chunk
			if (m_network.sendMessage(
					new InsertNameserviceEntriesResponse(p_insertIDRequest, null)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming GetChunkIDForNameserviceEntryRequest
	 *
	 * @param p_getChunkIDForNameserviceEntryRequest the GetChunkIDForNameserviceEntryRequest
	 */
	private void incomingGetChunkIDForNameserviceEntryRequest(
			final GetChunkIDForNameserviceEntryRequest p_getChunkIDForNameserviceEntryRequest) {
		int id;
		long chunkID = -1;

		id = p_getChunkIDForNameserviceEntryRequest.getID();
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Got request: GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST from "
				+ NodeID.toHexString(p_getChunkIDForNameserviceEntryRequest.getSource()) + ", id " + id);
		// #endif /* LOGGER == TRACE */

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(m_hashGenerator.hash(id), m_predecessor, m_nodeID,
				UPPER_CLOSED_INTERVAL)) {
			m_mappingLock.lock();
			chunkID = m_idTable.get(id);
			m_mappingLock.unlock();
			// #if LOGGER == TRACE
			m_logger.trace(getClass(),
					"GET_CHUNKID_REQUEST from " + NodeID.toHexString(p_getChunkIDForNameserviceEntryRequest.getSource())
							+ ", id " + id + ", reply chunkID " + ChunkID.toHexString(chunkID));
			// #endif /* LOGGER == TRACE */
		}
		if (m_network.sendMessage(
				new GetChunkIDForNameserviceEntryResponse(p_getChunkIDForNameserviceEntryRequest,
						chunkID)) != NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming GetNameserviceEntryCountRequest
	 *
	 * @param p_getNameserviceEntryCountRequest the GetNameserviceEntryCountRequest
	 */
	private void incomingGetNameserviceEntryCountRequest(
			final GetNameserviceEntryCountRequest p_getNameserviceEntryCountRequest) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Got request: GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST from "
				+ NodeID.toHexString(p_getNameserviceEntryCountRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		if (m_network.sendMessage(
				new GetNameserviceEntryCountResponse(p_getNameserviceEntryCountRequest,
						m_idTable.getNumberOfOwnEntries(m_predecessor, m_nodeID, m_superpeers.isEmpty(),
								UPPER_CLOSED_INTERVAL, m_hashGenerator))) != NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming GetNameserviceEntriesRequest
	 *
	 * @param p_getNameserviceEntriesRequest the GetNameserviceEntriesRequest
	 */
	private void incomingGetNameserviceEntriesRequest(
			final GetNameserviceEntriesRequest p_getNameserviceEntriesRequest) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Got request: GET_NAMESERVICE_ENTRIES from "
				+ p_getNameserviceEntriesRequest.getSource());
		// #endif /* LOGGER == TRACE */

		if (m_network.sendMessage(
				new GetNameserviceEntriesResponse(p_getNameserviceEntriesRequest,
						m_idTable.get())) != NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming MigrateRequest
	 *
	 * @param p_migrateRequest the MigrateRequest
	 */
	private void incomingMigrateRequest(final MigrateRequest p_migrateRequest) {
		short nodeID;
		long chunkID;
		short creator;
		short[] backupSuperpeers;
		LookupTree tree;
		MigrateRequest request;
		boolean isBackup;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"Got Message: MIGRATE_REQUEST from " + NodeID.toHexString(p_migrateRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		nodeID = p_migrateRequest.getNodeID();
		chunkID = p_migrateRequest.getChunkID();
		creator = ChunkID.getCreatorID(chunkID);
		isBackup = p_migrateRequest.isBackup();

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_dataLock.unlock();
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"CIDTree range not initialized on responsible superpeer " + NodeID.toHexString(m_nodeID));
				// #endif /* LOGGER >= ERROR */
				if (m_network.sendMessage(
						new MigrateResponse(p_migrateRequest, false)) != NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore request it
				}
			} else {
				tree.migrateObject(chunkID, nodeID);
				m_dataLock.unlock();

				m_overlayLock.lock();
				backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
				m_overlayLock.unlock();
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (short backupSuperpeer : backupSuperpeers) {
						request = new MigrateRequest(backupSuperpeer, chunkID, nodeID, BACKUP);
						if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
							// Ignore superpeer failure, superpeer will fix this later
						}
					}
				}
				if (m_network.sendMessage(
						new MigrateResponse(p_migrateRequest, true)) != NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				// #if LOGGER >= WARN
				m_logger.warn(getClass(),
						"CIDTree range not initialized on backup superpeer " + NodeID.toHexString(m_nodeID));
				// #endif /* LOGGER >= WARN */
			} else {
				tree.migrateObject(chunkID, nodeID);
			}
			m_dataLock.unlock();
			if (m_network.sendMessage(
					new MigrateResponse(p_migrateRequest, true)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}

		} else {
			// Not responsible for requesting peer
			if (m_network.sendMessage(
					new MigrateResponse(p_migrateRequest, false)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming MigrateRangeRequest
	 *
	 * @param p_migrateRangeRequest the MigrateRangeRequest
	 */
	private void incomingMigrateRangeRequest(final MigrateRangeRequest p_migrateRangeRequest) {
		short nodeID;
		long startChunkID;
		long endChunkID;
		short creator;
		short[] backupSuperpeers;
		LookupTree tree;
		MigrateRangeRequest request;
		boolean isBackup;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"Got Message: MIGRATE_RANGE_REQUEST from " + NodeID.toHexString(p_migrateRangeRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		nodeID = p_migrateRangeRequest.getNodeID();
		startChunkID = p_migrateRangeRequest.getStartChunkID();
		endChunkID = p_migrateRangeRequest.getEndChunkID();
		creator = ChunkID.getCreatorID(startChunkID);
		isBackup = p_migrateRangeRequest.isBackup();

		if (creator != ChunkID.getCreatorID(endChunkID)) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "start and end objects creators not equal");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_dataLock.unlock();
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"CIDTree range not initialized on responsible superpeer " + NodeID.toHexString(m_nodeID));
				// #endif /* LOGGER >= ERROR */
				if (m_network.sendMessage(
						new MigrateRangeResponse(p_migrateRangeRequest, false)) != NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			} else {
				tree.migrateRange(startChunkID, endChunkID, nodeID);
				m_dataLock.unlock();

				m_overlayLock.lock();
				backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
				m_overlayLock.unlock();
				if (-1 != backupSuperpeers[0]) {
					// Send backups
					for (short backupSuperpeer : backupSuperpeers) {
						request =
								new MigrateRangeRequest(backupSuperpeer, startChunkID, endChunkID, nodeID, BACKUP);
						if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
							// Ignore superpeer failure, superpeer will fix this later
						}
					}
				}
				if (m_network.sendMessage(
						new MigrateRangeResponse(p_migrateRangeRequest, true)) != NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				// #if LOGGER >= WARN
				m_logger.warn(getClass(),
						"CIDTree range not initialized on backup superpeer " + NodeID.toHexString(m_nodeID));
				// #endif /* LOGGER >= WARN */
			} else {
				tree.migrateRange(startChunkID, endChunkID, nodeID);
			}
			m_dataLock.unlock();
			if (m_network.sendMessage(
					new MigrateRangeResponse(p_migrateRangeRequest, true)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			if (m_network.sendMessage(
					new MigrateRangeResponse(p_migrateRangeRequest, false)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore request it
			}
		}
	}

	/**
	 * Handles an incoming InitRangeRequest
	 *
	 * @param p_initRangeRequest the InitRangeRequest
	 */
	private void incomingInitRangeRequest(final InitRangeRequest p_initRangeRequest) {
		LookupRangeWithBackupPeers primaryAndBackupPeers;
		long startChunkIDRangeID;
		short creator;
		short[] backupSuperpeers;
		LookupTree tree;
		InitRangeRequest request;
		boolean isBackup;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"Got Message: INIT_RANGE_REQUEST from " + NodeID.toHexString(p_initRangeRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		primaryAndBackupPeers = new LookupRangeWithBackupPeers(p_initRangeRequest.getLookupRange());
		startChunkIDRangeID = p_initRangeRequest.getStartChunkIDOrRangeID();
		creator = primaryAndBackupPeers.getPrimaryPeer();
		isBackup = p_initRangeRequest.isBackup();

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				tree = new LookupTree(ORDER);
				addCIDTree(creator, tree);
			}
			if (ChunkID.getCreatorID(startChunkIDRangeID) != -1) {
				tree.initRange(startChunkIDRangeID, creator, primaryAndBackupPeers.getBackupPeers());
			} else {
				tree.initMigrationRange((int) startChunkIDRangeID, primaryAndBackupPeers.getBackupPeers());
			}
			m_dataLock.unlock();

			m_overlayLock.lock();
			backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
			m_overlayLock.unlock();
			if (-1 != backupSuperpeers[0]) {
				// Send backups
				for (short backupSuperpeer : backupSuperpeers) {
					request = new InitRangeRequest(backupSuperpeer, startChunkIDRangeID,
							primaryAndBackupPeers.convertToLong(), BACKUP);
					if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
						// Ignore superpeer failure, superpeer will fix this later
					}
				}
			}
			if (m_network.sendMessage(
					new InitRangeResponse(p_initRangeRequest, true)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				tree = new LookupTree((short) 10);
				addCIDTree(creator, tree);
			}
			if ((startChunkIDRangeID & 0x0000FFFFFFFFFFFFL) != 0) {
				tree.initRange(startChunkIDRangeID, creator, primaryAndBackupPeers.getBackupPeers());
			} else {
				tree.initMigrationRange((int) startChunkIDRangeID, primaryAndBackupPeers.getBackupPeers());
			}
			m_dataLock.unlock();
			if (m_network.sendMessage(
					new InitRangeResponse(p_initRangeRequest, true)) != NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			if (m_network.sendMessage(
					new InitRangeResponse(p_initRangeRequest, false)) != NetworkErrorCodes.SUCCESS) {
				// Requesting node is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming GetAllBackupRangesRequest
	 *
	 * @param p_getAllBackupRangesRequest the GetAllBackupRangesRequest
	 */
	private void incomingGetAllBackupRangesRequest(final GetAllBackupRangesRequest p_getAllBackupRangesRequest) {
		int counter = 0;
		BackupRange[] result = null;
		LookupTree tree;
		ArrayList<long[]> ownBackupRanges;
		ArrayList<Long> migrationBackupRanges;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(),
				"Got request: GET_ALL_BACKUP_RANGES_REQUEST "
						+ NodeID.toHexString(p_getAllBackupRangesRequest.getSource()));
		// #endif /* LOGGER == TRACE */

		m_dataLock.lock();
		tree = getCIDTree(p_getAllBackupRangesRequest.getNodeID());
		if (tree != null) {
			ownBackupRanges = tree.getAllBackupRanges();
			migrationBackupRanges = tree.getAllMigratedBackupRanges();

			result = new BackupRange[ownBackupRanges.size() + migrationBackupRanges.size()];
			for (long[] backupRange : ownBackupRanges) {
				result[counter++] = new BackupRange(backupRange[0], backupRange[1]);
			}
			counter = 0;
			for (long backupRange : migrationBackupRanges) {
				result[counter + ownBackupRanges.size()] = new BackupRange(counter, backupRange);
				counter++;
			}
		}
		m_dataLock.unlock();
		if (m_network.sendMessage(
				new GetAllBackupRangesResponse(p_getAllBackupRangesRequest, result)) != NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming SetRestorerAfterRecoveryMessage
	 *
	 * @param p_setRestorerAfterRecoveryMessage the SetRestorerAfterRecoveryMessage
	 */
	private void incomingSetRestorerAfterRecoveryMessage(
			final SetRestorerAfterRecoveryMessage p_setRestorerAfterRecoveryMessage) {
		LookupTree tree;

		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Got request: SET_RESTORER_AFTER_RECOVERY_MESSAGE "
				+ NodeID.toHexString(p_setRestorerAfterRecoveryMessage.getSource()));
		// #endif /* LOGGER == TRACE */

		m_dataLock.lock();
		tree = getCIDTree(p_setRestorerAfterRecoveryMessage.getOwner());
		if (tree != null) {
			tree.setRestorer(p_setRestorerAfterRecoveryMessage.getSource());
		}
		m_dataLock.unlock();
	}

	/**
	 * Handles an incoming BarrierAllocRequest
	 *
	 * @param p_request the BarrierAllocRequest
	 */
	private void incomingBarrierAllocRequest(final BarrierAllocRequest p_request) {
		int barrierId = m_barriersTable.allocateBarrier(p_request.getBarrierSize());
		// #if LOGGER >= ERROR
		if (barrierId == BarrierID.INVALID_ID) {
			m_logger.error(getClass(), "Creating barrier for size " + p_request.getBarrierSize() + " failed.");
		}
		// #endif /* LOGGER >= ERROR */

		BarrierAllocResponse response = new BarrierAllocResponse(p_request, barrierId);
		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to barrier request " + p_request + " failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}

	/**
	 * Handles an incoming BarrierFreeRequest
	 *
	 * @param p_message the BarrierFreeRequest
	 */
	private void incomingBarrierFreeRequest(final BarrierFreeRequest p_message) {
		BarrierFreeResponse response = new BarrierFreeResponse(p_message);
		if (!m_barriersTable.freeBarrier(p_message.getBarrierId())) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(),
					"Free'ing barrier " + BarrierID.toHexString(p_message.getBarrierId()) + " failed.");
			// #endif /* LOGGER >= ERROR */
			response.setStatusCode((byte) -1);
		} else {
			response.setStatusCode((byte) 0);
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending back response for barrier free message " + p_message + " failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}

	/**
	 * Handles an incoming BarrierSignOnRequest
	 *
	 * @param p_request the BarrierSignOnRequest
	 */
	private void incomingBarrierSignOnRequest(final BarrierSignOnRequest p_request) {
		int barrierId = p_request.getBarrierId();
		int res = m_barriersTable.signOn(barrierId, p_request.getSource(), p_request.getCustomData());
		BarrierSignOnResponse response = new BarrierSignOnResponse(p_request, (byte) (res >= 0 ? 0 : -1));
		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to sign on request " + p_request + " failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */

		// release all if this was the last sign on
		if (res == 0) {
			short[] signedOnPeers = m_barriersTable.getSignedOnPeers(barrierId);
			long[] customData = m_barriersTable.getBarrierCustomData(barrierId);
			for (int i = 1; i < signedOnPeers.length; i++) {
				BarrierReleaseMessage message =
						new BarrierReleaseMessage(signedOnPeers[i], barrierId, signedOnPeers, customData);
				err = m_network.sendMessage(message);
				// #if LOGGER >= ERROR
				if (err != NetworkErrorCodes.SUCCESS) {
					m_logger.error(getClass(),
							"Releasing peer " + NodeID.toHexString(signedOnPeers[i]) + " of barrier " + BarrierID
									.toHexString(barrierId) + " failed: " + err);
				}
				// #endif /* LOGGER >= ERROR */
			}
			// reset for reuse
			m_barriersTable.reset(barrierId);
		}
	}

	/**
	 * Handles an incoming BarrierGetStatusRequest
	 *
	 * @param p_request the BarrierGetStatusRequest
	 */
	private void incomingBarrierGetStatusRequest(final BarrierGetStatusRequest p_request) {
		short[] signedOnPeers = m_barriersTable.getSignedOnPeers(p_request.getBarrierId());
		BarrierGetStatusResponse response;
		if (signedOnPeers == null) {
			// barrier does not exist
			response = new BarrierGetStatusResponse(p_request, new short[0]);
			response.setStatusCode((byte) -1);
		} else {
			response = new BarrierGetStatusResponse(p_request, signedOnPeers);
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to status request " + p_request + " failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}

	/**
	 * Handles an incoming BarrierChangeSizeRequest
	 *
	 * @param p_request the BarrierChangeSizeRequest
	 */
	private void incomingBarrierChangeSizeRequest(final BarrierChangeSizeRequest p_request) {
		BarrierChangeSizeResponse response = new BarrierChangeSizeResponse(p_request);
		if (!m_barriersTable.changeBarrierSize(p_request.getBarrierId(), p_request.getBarrierSize())) {
			response.setStatusCode((byte) -1);
		} else {
			response.setStatusCode((byte) 0);
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(),
					"Sending response for barrier change size request " + p_request + " failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}

	/**
	 * Handles an incoming SuperpeerStorageCreateRequest
	 *
	 * @param p_request the SuperpeerStorageCreateRequest
	 */
	private void incomingSuperpeerStorageCreateRequest(final SuperpeerStorageCreateRequest p_request) {
		m_storageLock.writeLock().lock();
		int ret = m_storage.create(p_request.getStorageId(), p_request.getSize());
		m_storageLock.writeLock().unlock();

		if (!p_request.isReplicate()) {
			SuperpeerStorageCreateResponse response = new SuperpeerStorageCreateResponse(p_request);
			response.setStatusCode((byte) ret);
			NetworkErrorCodes err = m_network.sendMessage(response);
			// #if LOGGER >= ERROR
			if (err != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(),
						"Sending response to storage create with size " + p_request.getSize() + " failed: " + err);
				return;
			}
			// #endif /* LOGGER >= ERROR */
		}

		// replicate to next 3 superpeers
		if (ret != 0 && !p_request.isReplicate()) {
			short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
			for (short backupSuperpeer : backupSuperpeers) {
				if (backupSuperpeer == NodeID.INVALID_ID) {
					continue;
				}

				SuperpeerStorageCreateRequest request =
						new SuperpeerStorageCreateRequest(backupSuperpeer, p_request.getStorageId(),
								p_request.getSize(), true);
				// send as message, only
				NetworkErrorCodes err = m_network.sendMessage(request);
				if (err != NetworkErrorCodes.SUCCESS) {
					// ignore result
				}
			}
		}
	}

	/**
	 * Handles an incoming SuperpeerStorageGetRequest
	 *
	 * @param p_request the SuperpeerStorageGetRequest
	 */
	private void incomingSuperpeerStorageGetRequest(final SuperpeerStorageGetRequest p_request) {
		m_storageLock.readLock().lock();
		byte[] data = m_storage.get(p_request.getStorageID());
		m_storageLock.readLock().unlock();

		Chunk chunk;
		if (data == null) {
			// create invalid entry
			chunk = new Chunk();
		} else {
			chunk = new Chunk(p_request.getStorageID(), ByteBuffer.wrap(data));
		}

		SuperpeerStorageGetResponse response = new SuperpeerStorageGetResponse(p_request, chunk);
		if (chunk.getID() == ChunkID.INVALID_ID) {
			response.setStatusCode((byte) -1);
		}

		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to storage get failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}

	/**
	 * Handles an incoming SuperpeerStoragePutRequest
	 *
	 * @param p_request the SuperpeerStoragePutRequest
	 */
	private void incomingSuperpeerStoragePutRequest(final SuperpeerStoragePutRequest p_request) {
		Chunk chunk = p_request.getChunk();

		m_storageLock.readLock().lock();
		int res = m_storage.put((int) chunk.getID(), chunk.getData().array());
		m_storageLock.readLock().unlock();

		if (!p_request.isReplicate()) {
			SuperpeerStoragePutResponse response = new SuperpeerStoragePutResponse(p_request);
			if (res != chunk.getDataSize()) {
				response.setStatusCode((byte) -1);
			}
			NetworkErrorCodes err = m_network.sendMessage(response);
			// #if LOGGER >= ERROR
			if (err != NetworkErrorCodes.SUCCESS) {
				m_logger.error(getClass(), "Sending response to put request to superpeer storage failed: " + err);
			}
			// #endif /* LOGGER >= ERROR */
		}

		// replicate to next 3 superpeers
		if (res == chunk.getDataSize() && !p_request.isReplicate()) {
			short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
			for (short backupSuperpeer : backupSuperpeers) {
				if (backupSuperpeer == NodeID.INVALID_ID) {
					continue;
				}
				SuperpeerStoragePutRequest request =
						new SuperpeerStoragePutRequest(backupSuperpeer, p_request.getChunk(), true);
				// send as message, only
				NetworkErrorCodes err = m_network.sendMessage(request);
				if (err != NetworkErrorCodes.SUCCESS) {
					// ignore result
				}
			}
		}
	}

	/**
	 * Handles an incoming SuperpeerStorageRemoveMessage
	 *
	 * @param p_request the SuperpeerStorageRemoveMessage
	 */
	private void incomingSuperpeerStorageRemoveMessage(final SuperpeerStorageRemoveMessage p_request) {
		m_storageLock.writeLock().lock();
		boolean res = m_storage.remove(p_request.getStorageId());
		// #if LOGGER >= ERROR
		if (!res) {
			m_logger.error(getClass(),
					"Removing object " + p_request.getStorageId() + " from superpeer storage failed.");
		}
		// #endif /* LOGGER >= ERROR */
		m_storageLock.writeLock().unlock();

		// replicate to next 3 superpeers
		if (res && !p_request.isReplicate()) {
			short[] backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
			for (short backupSuperpeer : backupSuperpeers) {
				if (backupSuperpeer == NodeID.INVALID_ID) {
					continue;
				}

				SuperpeerStorageRemoveMessage request =
						new SuperpeerStorageRemoveMessage(backupSuperpeer, p_request.getStorageId(), true);
				// send as message, only
				NetworkErrorCodes err = m_network.sendMessage(request);
				if (err != NetworkErrorCodes.SUCCESS) {
					// ignore result
				}
			}
		}
	}

	/**
	 * Handles an incoming SuperpeerStorageStatusRequest
	 *
	 * @param p_request the SuperpeerStorageStatusRequest
	 */
	private void incomingSuperpeerStorageStatusRequest(final SuperpeerStorageStatusRequest p_request) {
		m_storageLock.readLock().lock();
		SuperpeerStorage.Status status = m_storage.getStatus();
		m_storageLock.readLock().unlock();

		SuperpeerStorageStatusResponse response = new SuperpeerStorageStatusResponse(p_request, status);
		NetworkErrorCodes err = m_network.sendMessage(response);
		// #if LOGGER >= ERROR
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(getClass(), "Sending response to superpeer storage get status message failed: " + err);
		}
		// #endif /* LOGGER >= ERROR */
	}

	/**
	 * Handles an incoming Message
	 *
	 * @param p_message the Message
	 */
	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {
		if (p_message != null) {
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_JOIN_REQUEST:
						incomingJoinRequest((JoinRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST:
						incomingGetLookupRangeRequest((GetLookupRangeRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST:
						incomingRemoveChunkIDsRequest((RemoveChunkIDsRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST:
						incomingInsertNameserviceEntriesRequest((InsertNameserviceEntriesRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST:
						incomingGetChunkIDForNameserviceEntryRequest((GetChunkIDForNameserviceEntryRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST:
						incomingGetNameserviceEntryCountRequest((GetNameserviceEntryCountRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRIES_REQUEST:
						incomingGetNameserviceEntriesRequest((GetNameserviceEntriesRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_MIGRATE_REQUEST:
						incomingMigrateRequest((MigrateRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST:
						incomingMigrateRangeRequest((MigrateRangeRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_INIT_RANGE_REQUEST:
						incomingInitRangeRequest((InitRangeRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST:
						incomingGetAllBackupRangesRequest((GetAllBackupRangesRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE:
						incomingSetRestorerAfterRecoveryMessage((SetRestorerAfterRecoveryMessage) p_message);
						break;
					case LookupMessages.SUBTYPE_BARRIER_ALLOC_REQUEST:
						incomingBarrierAllocRequest((BarrierAllocRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_BARRIER_FREE_REQUEST:
						incomingBarrierFreeRequest((BarrierFreeRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_BARRIER_SIGN_ON_REQUEST:
						incomingBarrierSignOnRequest((BarrierSignOnRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_BARRIER_STATUS_REQUEST:
						incomingBarrierGetStatusRequest((BarrierGetStatusRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_BARRIER_CHANGE_SIZE_REQUEST:
						incomingBarrierChangeSizeRequest((BarrierChangeSizeRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_CREATE_REQUEST:
						incomingSuperpeerStorageCreateRequest((SuperpeerStorageCreateRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_GET_REQUEST:
						incomingSuperpeerStorageGetRequest((SuperpeerStorageGetRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_PUT_REQUEST:
						incomingSuperpeerStoragePutRequest((SuperpeerStoragePutRequest) p_message);
						break;
					case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_REMOVE_MESSAGE:
						incomingSuperpeerStorageRemoveMessage((SuperpeerStorageRemoveMessage) p_message);
						break;
					case LookupMessages.SUBTYPE_SUPERPEER_STORAGE_STATUS_REQUEST:
						incomingSuperpeerStorageStatusRequest((SuperpeerStorageStatusRequest) p_message);
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

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE,
				SendBackupsMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE,
				NotifyAboutFailedPeerMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE,
				StartRecoveryMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE,
				SetRestorerAfterRecoveryMessage.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE,
				PingSuperpeerMessage.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE,
				SendSuperpeersMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST,
				AskAboutBackupsRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE,
				AskAboutBackupsResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST,
				AskAboutSuccessorRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE,
				AskAboutSuccessorResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE,
				NotifyAboutNewPredecessorMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE,
				NotifyAboutNewSuccessorMessage.class);

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
		m_network.register(JoinRequest.class, this);
		m_network.register(GetLookupRangeRequest.class, this);
		m_network.register(RemoveChunkIDsRequest.class, this);
		m_network.register(InsertNameserviceEntriesRequest.class, this);
		m_network.register(GetChunkIDForNameserviceEntryRequest.class, this);
		m_network.register(GetNameserviceEntryCountRequest.class, this);
		m_network.register(GetNameserviceEntriesRequest.class, this);
		m_network.register(MigrateRequest.class, this);
		m_network.register(MigrateRangeRequest.class, this);
		m_network.register(InitRangeRequest.class, this);
		m_network.register(GetAllBackupRangesRequest.class, this);
		m_network.register(SetRestorerAfterRecoveryMessage.class, this);

		m_network.register(PingSuperpeerMessage.class, this);

		m_network.register(BarrierAllocRequest.class, this);
		m_network.register(BarrierFreeRequest.class, this);
		m_network.register(BarrierSignOnRequest.class, this);
		m_network.register(BarrierGetStatusRequest.class, this);
		m_network.register(BarrierChangeSizeRequest.class, this);

		m_network.register(SuperpeerStorageCreateRequest.class, this);
		m_network.register(SuperpeerStorageGetRequest.class, this);
		m_network.register(SuperpeerStoragePutRequest.class, this);
		m_network.register(SuperpeerStorageRemoveMessage.class, this);
		m_network.register(SuperpeerStorageStatusRequest.class, this);
	}
}
