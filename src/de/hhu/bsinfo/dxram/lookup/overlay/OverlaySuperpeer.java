
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.backup.BackupRange;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
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
import de.hhu.bsinfo.dxram.lookup.messages.GetAllBackupRangesRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetAllBackupRangesResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetChunkIDForNameserviceEntryRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetChunkIDForNameserviceEntryResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupRangeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupRangeResponse;
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
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.util.NodeID;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.utils.CRC16;

/**
 * Superpper functionality for overlay
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

	private SuperpeerStabilizationThread m_stabilizationThread;

	private ReentrantLock m_overlayLock;
	private ReentrantLock m_dataLock;
	private ReentrantLock m_mappingLock;
	private ReentrantLock m_failureLock;

	/**
	 * Creates an instance of OverlaySuperpeer
	 * @param p_nodeID
	 *            the own NodeID
	 * @param p_contactSuperpeer
	 *            the superpeer to contact for joining
	 * @param p_initialNumberOfSuperpeers
	 *            the number of expeced superpeers
	 * @param p_sleepInterval
	 *            the ping interval
	 * @param p_boot
	 *            the BootComponent
	 * @param p_logger
	 *            the LoggerComponent
	 * @param p_network
	 *            the NetworkComponent
	 * @param p_event
	 *            the EventComponent
	 */
	public OverlaySuperpeer(final short p_nodeID, final short p_contactSuperpeer, final int p_initialNumberOfSuperpeers, final int p_sleepInterval,
			final AbstractBootComponent p_boot, final LoggerComponent p_logger, final NetworkComponent p_network, final EventComponent p_event) {
		m_boot = p_boot;
		m_event = p_event;
		m_logger = p_logger;
		m_network = p_network;

		m_nodeID = p_nodeID;
		m_initialNumberOfSuperpeers = p_initialNumberOfSuperpeers;

		registerNetworkMessages();
		registerNetworkMessageListener();

		m_nodeTable = new LookupTree[65536];
		m_assignedPeersIncludingBackup = new ArrayList<Short>();
		m_idTable = new NameserviceHashTable(1000, 0.9f, m_logger);

		m_superpeers = new ArrayList<Short>();
		m_peers = new ArrayList<Short>();

		m_overlayLock = new ReentrantLock(false);
		m_dataLock = new ReentrantLock(false);
		m_mappingLock = new ReentrantLock(false);
		m_failureLock = new ReentrantLock(false);

		m_initialNumberOfSuperpeers--;

		createOrJoinSuperpeerOverlay(p_contactSuperpeer, p_sleepInterval);
	}

	/**
	 * Shuts down the stabilization thread
	 * @return
	 */
	public void shutdown() {
		m_stabilizationThread.interrupt();
		m_stabilizationThread.shutdown();
		try {
			m_stabilizationThread.join();
			m_logger.info(getClass(), "Shutdown of StabilizationThread successful.");
		} catch (final InterruptedException e) {
			m_logger.warn(getClass(), "Could not wait for stabilization thread to finish. Interrupted.");
		}
	}

	/**
	 * Returns whether this superpeer is last in overlay or not
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
	 * @return the predecessor
	 */
	protected short getPredecessor() {
		return m_predecessor;
	}

	/**
	 * Sets the predecessor for the current superpeer
	 * @param p_nodeID
	 *            NodeID of the predecessor
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
	 * @return the sucessor
	 */
	protected short getSuccessor() {
		return m_successor;
	}

	/**
	 * Sets the successor for the current superpeer
	 * @param p_nodeID
	 *            NodeID of the successor
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
	 * @return all peers
	 */
	protected ArrayList<Short> getPeers() {
		return m_peers;
	}

	/**
	 * Determines all peers that are in the responsible area
	 * @param p_oldSuperpeer
	 *            the old superpeer
	 * @param p_currentSuperpeer
	 *            the new superpeer
	 * @return all peers in responsible area
	 */
	protected ArrayList<Short> getPeersInResponsibleArea(final short p_oldSuperpeer, final short p_currentSuperpeer) {
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
	 * @param p_failedPeer
	 *            the failed peer
	 */
	protected void removeFailedPeer(final short p_failedPeer) {
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
	 * @param p_peers
	 *            all peers the requesting superpeer stores backups for
	 * @param p_trees
	 *            an empty ArrayList to put missing LookupTrees in
	 * @return the backup data of missing peers in given peer list
	 */
	protected byte[] compareAndReturnBackups(final ArrayList<Short> p_peers, final ArrayList<LookupTree> p_trees) {
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
					m_logger.trace(getClass(), "Spreading meta-data of " + currentPeer + " to " + m_successor);
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
	 * @param p_trees
	 *            the new LookupTrees
	 * @param p_mappings
	 *            the new mappings
	 */
	protected void storeIncomingBackups(final ArrayList<LookupTree> p_trees, final byte[] p_mappings) {
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
	 * @param p_responsibleArea
	 *            the responsible area
	 * @note assumes m_overlayLock has been locked
	 * @note is called periodically
	 */
	protected void deleteUnnecessaryBackups(final short[] p_responsibleArea) {
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
	 * @param p_nodeID
	 *            the NodeID
	 */
	protected void takeOverPeersAndCIDTrees(final short p_nodeID) {
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
					if (0 > Collections.binarySearch(m_peers, currentPeer) && 0 > Collections.binarySearch(m_superpeers, currentPeer)) {
						m_logger.info(getClass(), "** Taking over " + currentPeer);
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
	 * @param p_failedNode
	 *            the failed nodes NodeID
	 */
	protected void failureHandling(final short p_failedNode) {
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

			m_logger.error(getClass(), "********** ********** Node Failure ********** **********");

			// Check if failed node is a superpeer
			if (0 <= Collections.binarySearch(m_superpeers, p_failedNode)) {
				m_overlayLock.unlock();

				m_logger.error(getClass(), "Failed node was a superpeer, NodeID: " + p_failedNode);

				// notify others about failure
				m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_failedNode, NodeRole.SUPERPEER));

				// Determine new bootstrap if failed node is current one
				if (p_failedNode == m_bootstrap) {
					determineNewBootstrap();
					m_logger.info(getClass(), "Failed node " + p_failedNode + " was bootstrap. New bootstrap is " + m_bootstrap);
				}
				// Take over failed nodes peers and CIDTrees if it is this nodes predecessor
				if (p_failedNode == m_predecessor) {
					m_logger.info(getClass(), "Failed node " + p_failedNode + " was my predecessor -> taking over all peers and data");
					takeOverPeersAndCIDTrees(m_predecessor);
				}
				// Send failed nodes CIDTrees to this nodes successor if it is the first node in responsible area
				m_overlayLock.lock();
				responsibleArea = OverlayHelper.getResponsibleArea(m_nodeID, m_predecessor, m_superpeers);
				m_overlayLock.unlock();
				if (3 < m_superpeers.size()
						&& OverlayHelper.getResponsibleSuperpeer((short) (responsibleArea[0] + 1), m_superpeers, m_overlayLock, m_logger) == p_failedNode) {
					m_logger.info(getClass(), "Failed node " + p_failedNode + " was in my responsible area -> spreading his data");
					spreadDataOfFailedSuperpeer(p_failedNode, responsibleArea);
				}
				// Send this nodes CIDTrees to new backup node that replaces the failed node
				m_overlayLock.lock();
				backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
				m_overlayLock.unlock();
				if (3 < m_superpeers.size() && OverlayHelper.isNodeInRange(p_failedNode, backupSuperpeers[0], backupSuperpeers[2], CLOSED_INTERVAL)) {
					m_logger.info(getClass(), "Failed node " + p_failedNode + " was one of my backup nodes -> spreading my data");
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
					m_logger.error(getClass(), "Failed node was a terminal, NodeID: " + p_failedNode);

					// Remove peer
					m_overlayLock.lock();
					OverlayHelper.removePeer(p_failedNode, m_peers);
					m_overlayLock.unlock();

					m_logger.info(getClass(), "Failed node " + p_failedNode + ": no further actions required");

					// notify others about failure
					m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_failedNode, NodeRole.TERMINAL));
				} else {
					// Failed node was a peer
					m_logger.error(getClass(), "Failed node was a peer, NodeID: " + p_failedNode);

					// notify others about failure
					m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_failedNode, NodeRole.PEER));

					// Remove peer in meta-data (and replace with new backup node; DUMMY element currently)
					m_logger.info(getClass(), "Removing " + p_failedNode + " from local meta-data");
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
						m_logger.info(getClass(), "Informing " + superpeer + " to remove " + p_failedNode + " from meta-data");
						if (m_network.sendMessage(
								new NotifyAboutFailedPeerMessage(superpeer, p_failedNode))
								!= NetworkErrorCodes.SUCCESS) {
							// Superpeer is not available anymore, remove from superpeer array and continue
							m_logger.error(getClass(), "superpeer failed, too");
							m_failureLock.unlock();
							failureHandling(superpeer);
							m_failureLock.lock();
						}
					}

					// Start recovery
					m_logger.info(getClass(), "Starting recovery for failed node " + p_failedNode);
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

									m_logger.info(getClass(), "Starting recovery (not implemented, no execution)");
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

					m_logger.info(getClass(), "Recovery of failed node " + p_failedNode + " complete.");
				}
			}
		}
	}

	/**
	 * Joins the superpeer overlay through contactSuperpeer
	 * @param p_contactSuperpeer
	 *            NodeID of a known superpeer
	 * @param p_sleepInterval
	 *            the ping interval
	 * @return whether the joining was successful
	 */
	private boolean createOrJoinSuperpeerOverlay(final short p_contactSuperpeer, final int p_sleepInterval) {
		short contactSuperpeer;
		JoinRequest joinRequest = null;
		JoinResponse joinResponse = null;
		ArrayList<LookupTree> trees;
		LookupTree tree;

		m_logger.trace(getClass(), "Entering createOrJoinSuperpeerOverlay with: p_contactSuperpeer=" + p_contactSuperpeer);

		contactSuperpeer = p_contactSuperpeer;

		if (p_contactSuperpeer == NodeID.INVALID_ID) {
			m_logger.error(getClass(), "Cannot join superpeer overlay, no bootstrap superpeer available to contact.");
			return false;
		}

		if (m_nodeID == contactSuperpeer) {
			if (m_boot.getNodeRole().equals(NodeRole.SUPERPEER)) {
				m_logger.trace(getClass(), "Setting up new ring, I am " + m_nodeID);
				setSuccessor(m_nodeID);
			} else {
				m_logger.error(getClass(), "Bootstrap has to be a superpeer, exiting now.");
				return false;
			}
		} else {
			while (-1 != contactSuperpeer) {
				m_logger.trace(getClass(), "Contacting " + contactSuperpeer + " to join the ring, I am " + m_nodeID);

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
			for (int i = 0; i < trees.size(); i++) {
				tree = trees.get(i);
				addCIDTree(tree.getCreator(), trees.get(i));
			}
			m_idTable.putAll(joinResponse.getMappings());

			setSuccessor(joinResponse.getSuccessor());
			setPredecessor(joinResponse.getPredecessor());
		}

		m_logger.trace(getClass(), "Starting stabilization thread");
		m_stabilizationThread = new SuperpeerStabilizationThread(this, m_nodeID, m_overlayLock, m_initialNumberOfSuperpeers,
				m_superpeers, p_sleepInterval, m_logger, m_network);
		m_stabilizationThread.setName(SuperpeerStabilizationThread.class.getSimpleName() + " for " + LookupComponent.class.getSimpleName());
		m_stabilizationThread.setDaemon(true);
		m_stabilizationThread.start();

		m_logger.trace(getClass(), "Exiting createOrJoinSuperpeerOverlay");

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
					new PingSuperpeerMessage(m_bootstrap))
					!= NetworkErrorCodes.SUCCESS) {
				// New bootstrap is not available, start failure handling to
				// remove bootstrap from superpeer array and to determine a new bootstrap
				m_logger.error(getClass(), "new bootstrap failed, too");
				m_failureLock.unlock();
				failureHandling(m_bootstrap);
				m_failureLock.lock();
			}
		}
	}

	/**
	 * Returns the requested CIDTree
	 * @param p_nodeID
	 *            NodeID for that the CIDTree is requested
	 * @return the CIDTree for given NodeID
	 * @note assumes m_dataLock has been locked
	 */
	private LookupTree getCIDTree(final short p_nodeID) {
		return m_nodeTable[p_nodeID & 0xFFFF];
	}

	/**
	 * Adds the given CIDTree to NIDTable
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_tree
	 *            the CIDTree to add
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
	 * @param p_nodeID
	 *            the NodeID
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
	 * @param p_nodeID
	 *            the NodeID
	 * @param p_responsibleArea
	 *            the responsible area
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
			allMappings = m_idTable.toArray(p_responsibleArea[0], p_nodeID, false, UPPER_CLOSED_INTERVAL, m_hashGenerator);
			m_mappingLock.unlock();
		}
		m_dataLock.unlock();

		while (!m_superpeers.isEmpty()) {
			m_logger.info(getClass(), "Spreading failed superpeers meta-data to " + m_successor);
			if (m_network.sendMessage(
					new SendBackupsMessage(m_successor, allMappings, trees))
					!= NetworkErrorCodes.SUCCESS) {
				// Successor is not available anymore, remove from superpeer array and try next superpeer
				m_logger.error(getClass(), "successor failed, too");
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
	 * @param p_backupSuperpeers
	 *            the current backup superpeers
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
				str += currentPeer;

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
			str += " to " + newBackupSuperpeer;

			if (m_network.sendMessage(
					new SendBackupsMessage(newBackupSuperpeer, allMappings, trees))
					!= NetworkErrorCodes.SUCCESS) {
				// Superpeer is not available anymore, remove from superpeer array and try next superpeer
				m_logger.error(getClass(), "new backup superpeer (" + newBackupSuperpeer + ") failed, too");
				m_failureLock.unlock();
				failureHandling(newBackupSuperpeer);
				m_failureLock.lock();
				continue;
			}
			break;
		}
		if (dataToTransmit && superpeerToSendData) {
			m_logger.info(getClass(), str);
		} else {
			m_logger.info(getClass(), "No need to spread data");
		}
	}

	/**
	 * Handles an incoming JoinRequest
	 * @param p_joinRequest
	 *            the JoinRequest
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

		m_logger.trace(getClass(), "Got request: JOIN_REQUEST from " + p_joinRequest.getSource());

		joiningNode = p_joinRequest.getNewNode();
		newNodeisSuperpeer = p_joinRequest.nodeIsSuperpeer();

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(joiningNode, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
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
				peers = new ArrayList<Short>();
				while (iter.hasNext()) {
					currentPeer = iter.next();
					if (OverlayHelper.isNodeInRange(currentPeer, joiningNodesPredecessor, joiningNode, OPEN_INTERVAL)) {
						peers.add(currentPeer);
					}
				}

				m_dataLock.lock();
				trees = new ArrayList<LookupTree>();
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
					while (OverlayHelper.isNodeInRange(currentPeer, responsibleArea[0], responsibleArea[1], OPEN_INTERVAL)) {
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
				mappings = m_idTable.toArray(responsibleArea[0], responsibleArea[1], m_superpeers.isEmpty(), UPPER_CLOSED_INTERVAL, m_hashGenerator);
				m_mappingLock.unlock();

				if (m_network.sendMessage(new JoinResponse(p_joinRequest, (short) -1, joiningNodesPredecessor, m_nodeID, mappings, m_superpeers, peers, trees))
						!= NetworkErrorCodes.SUCCESS) {
					// Joining node is not available anymore -> ignore request and return directly
					m_overlayLock.unlock();
					return;
				}

				for (int i = 0; i < peers.size(); i++) {
					OverlayHelper.removePeer(peers.get(i), m_peers);
				}

				// Notify predecessor about the joining node
				if (m_superpeers.isEmpty()) {
					setSuccessor(joiningNode);
					setPredecessor(joiningNode);
					m_overlayLock.unlock();
				} else {
					setPredecessor(joiningNode);
					m_overlayLock.unlock();

					if (m_network.sendMessage(new NotifyAboutNewSuccessorMessage(joiningNodesPredecessor, m_predecessor)) != NetworkErrorCodes.SUCCESS) {
						// Old predecessor is not available anymore, ignore it
					}
				}
			} else {
				m_overlayLock.lock();
				OverlayHelper.insertPeer(joiningNode, m_peers);
				if (m_network.sendMessage(
						new JoinResponse(p_joinRequest, (short) -1, (short) -1, (short) -1, null, m_superpeers, null, null)) != NetworkErrorCodes.SUCCESS) {
					// Joining node is not available anymore, ignore request
				}

				m_overlayLock.unlock();
			}
		} else {
			superpeer = OverlayHelper.getResponsibleSuperpeer(joiningNode, m_superpeers, m_overlayLock, m_logger);
			if (m_network.sendMessage(
					new JoinResponse(p_joinRequest, superpeer, (short) -1, (short) -1, null, null, null, null)) != NetworkErrorCodes.SUCCESS) {
				// Joining node is not available anymore, ignore request
			}
		}
	}

	/**
	 * Handles an incoming GetLookupRangeRequest
	 * @param p_getLookupRangeRequest
	 *            the GetLookupRangeRequest
	 */
	private void incomingGetLookupRangeRequest(final GetLookupRangeRequest p_getLookupRangeRequest) {
		long chunkID;
		LookupRange result = null;
		LookupTree tree;

		chunkID = p_getLookupRangeRequest.getChunkID();
		m_logger.trace(getClass(), "Got request: GET_LOOKUP_RANGE_REQUEST " + p_getLookupRangeRequest.getSource() + " chunkID: " + Long.toHexString(chunkID));

		m_dataLock.lock();
		tree = getCIDTree(ChunkID.getCreatorID(chunkID));
		if (null != tree) {
			result = tree.getMetadata(chunkID);
		}
		m_dataLock.unlock();

		m_logger.trace(getClass(), "GET_LOOKUP_RANGE_REQUEST " + p_getLookupRangeRequest.getSource() + " chunkID " + Long.toHexString(chunkID)
				+ " reply location: " + result);

		if (m_network.sendMessage(
				new GetLookupRangeResponse(p_getLookupRangeRequest, result))
				!= NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming RemoveChunkIDsRequest
	 * @param p_removeChunkIDsRequest
	 *            the RemoveChunkIDsRequest
	 */
	private void incomingRemoveChunkIDsRequest(final RemoveChunkIDsRequest p_removeChunkIDsRequest) {
		long[] chunkIDs;
		short creator;
		short[] backupSuperpeers;
		boolean isBackup;
		LookupTree tree;

		m_logger.trace(getClass(), "Got Message: REMOVE_CHUNKIDS_REQUEST from " + p_removeChunkIDsRequest.getSource());

		chunkIDs = p_removeChunkIDsRequest.getChunkIDs();
		isBackup = p_removeChunkIDsRequest.isBackup();

		for (long chunkID : chunkIDs) {
			creator = ChunkID.getCreatorID(chunkID);
			if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
				m_dataLock.lock();
				tree = getCIDTree(creator);
				if (null == tree) {
					m_dataLock.unlock();
					m_logger.error(getClass(), "CIDTree range not initialized on responsible superpeer " + m_nodeID);
					if (m_network.sendMessage(
							new RemoveChunkIDsResponse(p_removeChunkIDsRequest, new short[] {-1}))
							!= NetworkErrorCodes.SUCCESS) {
						// Requesting peer is not available anymore, ignore it
					}
				} else {
					tree.removeObject(chunkID);
					m_dataLock.unlock();

					m_overlayLock.lock();
					backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
					m_overlayLock.unlock();
					if (m_network.sendMessage(
							new RemoveChunkIDsResponse(p_removeChunkIDsRequest, backupSuperpeers))
							!= NetworkErrorCodes.SUCCESS) {
						// Requesting peer is not available anymore, ignore it
					}
				}
			} else if (isBackup) {
				m_dataLock.lock();
				tree = getCIDTree(creator);
				if (null == tree) {
					m_logger.warn(getClass(), "CIDTree range not initialized on backup superpeer " + m_nodeID);
				} else {
					tree.removeObject(chunkID);
				}
				m_dataLock.unlock();
				if (m_network.sendMessage(
						new RemoveChunkIDsResponse(p_removeChunkIDsRequest, null))
						!= NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			} else {
				// Not responsible for requesting peer
				if (m_network.sendMessage(
						new RemoveChunkIDsResponse(p_removeChunkIDsRequest, null))
						!= NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		}
	}

	/**
	 * Handles an incoming InsertIDRequest
	 * @param p_insertIDRequest
	 *            the InsertIDRequest
	 */
	private void incomingInsertNameserviceEntriesRequest(final InsertNameserviceEntriesRequest p_insertIDRequest) {
		int id;
		short[] backupSuperpeers;

		id = p_insertIDRequest.getID();
		m_logger.trace(getClass(), "Got request: INSERT_ID_REQUEST from " + p_insertIDRequest.getSource() + ", id " + id);

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(m_hashGenerator.hash(id), m_predecessor, m_nodeID, UPPER_CLOSED_INTERVAL)) {
			m_mappingLock.lock();
			m_idTable.put(id, p_insertIDRequest.getChunkID());
			m_mappingLock.unlock();

			m_overlayLock.lock();
			backupSuperpeers = OverlayHelper.getBackupSuperpeers(m_nodeID, m_superpeers);
			m_overlayLock.unlock();
			if (m_network.sendMessage(
					new InsertNameserviceEntriesResponse(p_insertIDRequest, backupSuperpeers))
					!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else if (p_insertIDRequest.isBackup()) {
			m_mappingLock.lock();
			m_idTable.put(id, p_insertIDRequest.getChunkID());
			m_mappingLock.unlock();

			if (m_network.sendMessage(
					new InsertNameserviceEntriesResponse(p_insertIDRequest, null))
					!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for that chunk
			if (m_network.sendMessage(
					new InsertNameserviceEntriesResponse(p_insertIDRequest, null))
					!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming GetChunkIDForNameserviceEntryRequest
	 * @param p_getChunkIDForNameserviceEntryRequest
	 *            the GetChunkIDForNameserviceEntryRequest
	 */
	private void incomingGetChunkIDForNameserviceEntryRequest(final GetChunkIDForNameserviceEntryRequest p_getChunkIDForNameserviceEntryRequest) {
		int id;
		long chunkID = -1;

		id = p_getChunkIDForNameserviceEntryRequest.getID();
		m_logger.trace(getClass(), "Got request: GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST from " + p_getChunkIDForNameserviceEntryRequest.getSource()
				+ ", id " + id);

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(m_hashGenerator.hash(id), m_predecessor, m_nodeID, UPPER_CLOSED_INTERVAL)) {
			m_mappingLock.lock();
			chunkID = m_idTable.get(id);
			m_mappingLock.unlock();
			m_logger.trace(
					getClass(),
					"GET_CHUNKID_REQUEST from " + p_getChunkIDForNameserviceEntryRequest.getSource() + ", id " + id + ", reply chunkID "
							+ Long.toHexString(chunkID));
		}
		if (m_network.sendMessage(
				new GetChunkIDForNameserviceEntryResponse(p_getChunkIDForNameserviceEntryRequest, chunkID))
				!= NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming GetNameserviceEntryCountRequest
	 * @param p_getNameserviceEntryCountRequest
	 *            the GetNameserviceEntryCountRequest
	 */
	private void incomingGetNameserviceEntryCountRequest(final GetNameserviceEntryCountRequest p_getNameserviceEntryCountRequest) {
		m_logger.trace(getClass(), "Got request: GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST from " + p_getNameserviceEntryCountRequest.getSource());

		if (m_network.sendMessage(
				new GetNameserviceEntryCountResponse(p_getNameserviceEntryCountRequest,
						m_idTable.getNumberOfOwnEntries(m_predecessor, m_nodeID, m_superpeers.isEmpty(), UPPER_CLOSED_INTERVAL, m_hashGenerator)))
						!= NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming MigrateRequest
	 * @param p_migrateRequest
	 *            the MigrateRequest
	 */
	private void incomingMigrateRequest(final MigrateRequest p_migrateRequest) {
		short nodeID;
		long chunkID;
		short creator;
		short[] backupSuperpeers;
		LookupTree tree;
		MigrateRequest request;
		boolean isBackup;

		m_logger.trace(getClass(), "Got Message: MIGRATE_REQUEST from " + p_migrateRequest.getSource());

		nodeID = p_migrateRequest.getNodeID();
		chunkID = p_migrateRequest.getChunkID();
		creator = ChunkID.getCreatorID(chunkID);
		isBackup = p_migrateRequest.isBackup();

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_dataLock.unlock();
				m_logger.error(getClass(), "CIDTree range not initialized on responsible superpeer " + m_nodeID);
				if (m_network.sendMessage(
						new MigrateResponse(p_migrateRequest, false))
				!= NetworkErrorCodes.SUCCESS) {
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
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new MigrateRequest(backupSuperpeers[i], chunkID, nodeID, BACKUP);
						if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
							// Ignore superpeer failure, superpeer will fix this later
							continue;
						}
					}
				}
				if (m_network.sendMessage(
						new MigrateResponse(p_migrateRequest, true))
				!= NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_logger.warn(getClass(), "CIDTree range not initialized on backup superpeer " + m_nodeID);
			} else {
				tree.migrateObject(chunkID, nodeID);
			}
			m_dataLock.unlock();
			if (m_network.sendMessage(
					new MigrateResponse(p_migrateRequest, true))
				!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}

		} else {
			// Not responsible for requesting peer
			if (m_network.sendMessage(
					new MigrateResponse(p_migrateRequest, false))
				!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming MigrateRangeRequest
	 * @param p_migrateRangeRequest
	 *            the MigrateRangeRequest
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

		m_logger.trace(getClass(), "Got Message: MIGRATE_RANGE_REQUEST from " + p_migrateRangeRequest.getSource());

		nodeID = p_migrateRangeRequest.getNodeID();
		startChunkID = p_migrateRangeRequest.getStartChunkID();
		endChunkID = p_migrateRangeRequest.getEndChunkID();
		creator = ChunkID.getCreatorID(startChunkID);
		isBackup = p_migrateRangeRequest.isBackup();

		if (creator != ChunkID.getCreatorID(endChunkID)) {
			m_logger.error(getClass(), "start and end objects creators not equal");
			return;
		}

		if (m_superpeers.isEmpty() || OverlayHelper.isNodeInRange(creator, m_predecessor, m_nodeID, OPEN_INTERVAL)) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_dataLock.unlock();
				m_logger.error(getClass(), "CIDTree range not initialized on responsible superpeer " + m_nodeID);
				if (m_network.sendMessage(
						new MigrateRangeResponse(p_migrateRangeRequest, false))
				!= NetworkErrorCodes.SUCCESS) {
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
					for (int i = 0; i < backupSuperpeers.length; i++) {
						request = new MigrateRangeRequest(backupSuperpeers[i], startChunkID, endChunkID, nodeID, BACKUP);
						if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
							// Ignore superpeer failure, superpeer will fix this later
							continue;
						}
					}
				}
				if (m_network.sendMessage(
						new MigrateRangeResponse(p_migrateRangeRequest, true))
				!= NetworkErrorCodes.SUCCESS) {
					// Requesting peer is not available anymore, ignore it
				}
			}
		} else if (isBackup) {
			m_dataLock.lock();
			tree = getCIDTree(creator);
			if (null == tree) {
				m_logger.warn(getClass(), "CIDTree range not initialized on backup superpeer " + m_nodeID);
			} else {
				tree.migrateRange(startChunkID, endChunkID, nodeID);
			}
			m_dataLock.unlock();
			if (m_network.sendMessage(
					new MigrateRangeResponse(p_migrateRangeRequest, true))
				!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			if (m_network.sendMessage(
					new MigrateRangeResponse(p_migrateRangeRequest, false))
				!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore request it
			}
		}
	}

	/**
	 * Handles an incoming InitRangeRequest
	 * @param p_initRangeRequest
	 *            the InitRangeRequest
	 */
	private void incomingInitRangeRequest(final InitRangeRequest p_initRangeRequest) {
		LookupRangeWithBackupPeers primaryAndBackupPeers;
		long startChunkIDRangeID;
		short creator;
		short[] backupSuperpeers;
		LookupTree tree;
		InitRangeRequest request;
		boolean isBackup;

		m_logger.trace(getClass(), "Got Message: INIT_RANGE_REQUEST from " + p_initRangeRequest.getSource());

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
				for (int i = 0; i < backupSuperpeers.length; i++) {
					request = new InitRangeRequest(backupSuperpeers[i], startChunkIDRangeID, primaryAndBackupPeers.convertToLong(), BACKUP);
					if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
						// Ignore superpeer failure, superpeer will fix this later
						continue;
					}
				}
			}
			if (m_network.sendMessage(
					new InitRangeResponse(p_initRangeRequest, true))
					!= NetworkErrorCodes.SUCCESS) {
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
					new InitRangeResponse(p_initRangeRequest, true))
					!= NetworkErrorCodes.SUCCESS) {
				// Requesting peer is not available anymore, ignore it
			}
		} else {
			// Not responsible for requesting peer
			if (m_network.sendMessage(
					new InitRangeResponse(p_initRangeRequest, false))
					!= NetworkErrorCodes.SUCCESS) {
				// Requesting node is not available anymore, ignore it
			}
		}
	}

	/**
	 * Handles an incoming GetAllBackupRangesRequest
	 * @param p_getAllBackupRangesRequest
	 *            the GetAllBackupRangesRequest
	 */
	private void incomingGetAllBackupRangesRequest(final GetAllBackupRangesRequest p_getAllBackupRangesRequest) {
		int counter = 0;
		BackupRange[] result = null;
		LookupTree tree;
		ArrayList<long[]> ownBackupRanges;
		ArrayList<Long> migrationBackupRanges;

		m_logger.trace(getClass(), "Got request: GET_ALL_BACKUP_RANGES_REQUEST " + p_getAllBackupRangesRequest.getSource());

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
				new GetAllBackupRangesResponse(p_getAllBackupRangesRequest, result))
				!= NetworkErrorCodes.SUCCESS) {
			// Requesting peer is not available anymore, ignore it
		}
	}

	/**
	 * Handles an incoming SetRestorerAfterRecoveryMessage
	 * @param p_setRestorerAfterRecoveryMessage
	 *            the SetRestorerAfterRecoveryMessage
	 */
	private void incomingSetRestorerAfterRecoveryMessage(final SetRestorerAfterRecoveryMessage p_setRestorerAfterRecoveryMessage) {
		LookupTree tree;

		m_logger.trace(getClass(), "Got request: SET_RESTORER_AFTER_RECOVERY_MESSAGE " + p_setRestorerAfterRecoveryMessage.getSource());

		m_dataLock.lock();
		tree = getCIDTree(p_setRestorerAfterRecoveryMessage.getOwner());
		if (tree != null) {
			tree.setRestorer(p_setRestorerAfterRecoveryMessage.getSource());
		}
		m_dataLock.unlock();
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
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_REQUEST, GetLookupRangeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_RANGE_RESPONSE, GetLookupRangeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_REQUEST, RemoveChunkIDsRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REMOVE_CHUNKIDS_RESPONSE, RemoveChunkIDsResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_REQUEST, InsertNameserviceEntriesRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INSERT_NAMESERVICE_ENTRIES_RESPONSE, InsertNameserviceEntriesResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_REQUEST,
				GetChunkIDForNameserviceEntryRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_CHUNKID_FOR_NAMESERVICE_ENTRY_RESPONSE,
				GetChunkIDForNameserviceEntryResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_REQUEST, GetNameserviceEntryCountRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_NAMESERVICE_ENTRY_COUNT_RESPONSE, GetNameserviceEntryCountResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_REQUEST, MigrateRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RESPONSE, MigrateResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, MigrateRangeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, MigrateRangeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, InitRangeRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE, InitRangeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_REQUEST, GetAllBackupRangesRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_GET_ALL_BACKUP_RANGES_RESPONSE, GetAllBackupRangesResponse.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, SendBackupsMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE, NotifyAboutFailedPeerMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE, StartRecoveryMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SET_RESTORER_AFTER_RECOVERY_MESSAGE, SetRestorerAfterRecoveryMessage.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, PingSuperpeerMessage.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, SendSuperpeersMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, AskAboutBackupsRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, AskAboutBackupsResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, AskAboutSuccessorRequest.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, AskAboutSuccessorResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE, NotifyAboutNewPredecessorMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, NotifyAboutNewSuccessorMessage.class);
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
		m_network.register(MigrateRequest.class, this);
		m_network.register(MigrateRangeRequest.class, this);
		m_network.register(InitRangeRequest.class, this);
		m_network.register(GetAllBackupRangesRequest.class, this);
		m_network.register(SetRestorerAfterRecoveryMessage.class, this);

		m_network.register(PingSuperpeerMessage.class, this);
	}
}
