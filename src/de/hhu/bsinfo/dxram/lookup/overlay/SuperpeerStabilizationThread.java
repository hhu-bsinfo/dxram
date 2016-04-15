
package de.hhu.bsinfo.dxram.lookup.overlay;

import java.util.ArrayList;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutBackupsResponse;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorRequest;
import de.hhu.bsinfo.dxram.lookup.messages.AskAboutSuccessorResponse;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutFailedPeerMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewPredecessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.NotifyAboutNewSuccessorMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SendBackupsMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SendSuperpeersMessage;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Stabilizes superpeer overlay
 * @author Kevin Beineke
 *         03.06.2013
 */
class SuperpeerStabilizationThread extends Thread implements MessageReceiver {

	// Constants
	private static final short OPEN_INTERVAL = 2;

	// Attributes
	private LoggerComponent m_logger;
	private NetworkComponent m_network;

	private OverlaySuperpeer m_superpeer;

	private int m_initialNumberOfSuperpeers;
	private ArrayList<Short> m_otherSuperpeers;
	private ReentrantLock m_overlayLock;

	private short m_nodeID;
	private int m_sleepInterval;
	private int m_next;
	private boolean m_shutdown;

	private String m_overlayFigure;

	// Constructors
	/**
	 * Creates an instance of Worker
	 * @param p_superpeer
	 *            the overlay superpeer
	 * @param p_nodeID
	 *            the own NodeID
	 * @param p_overlayLock
	 *            the overlay lock
	 * @param p_initialNumberOfSuperpeers
	 *            the number of expected superpeers
	 * @param p_superpeers
	 *            all other superpeers
	 * @param p_sleepInterval
	 *            the ping interval
	 * @param p_logger
	 *            the logger component
	 * @param p_network
	 *            the network component
	 */
	protected SuperpeerStabilizationThread(final OverlaySuperpeer p_superpeer, final short p_nodeID, final ReentrantLock p_overlayLock,
			final int p_initialNumberOfSuperpeers, final ArrayList<Short> p_superpeers, final int p_sleepInterval, final LoggerComponent p_logger,
			final NetworkComponent p_network) {
		m_superpeer = p_superpeer;

		m_logger = p_logger;
		m_network = p_network;

		m_otherSuperpeers = p_superpeers;
		m_overlayLock = p_overlayLock;

		m_nodeID = p_nodeID;
		m_sleepInterval = p_sleepInterval;
		m_next = 0;

		registerNetworkMessageListener();
	}

	/**
	 * Shutdown
	 */
	protected void shutdown() {
		m_shutdown = true;
	}

	/**
	 * When an object implementing interface <code>Runnable</code> is used
	 * to create a thread, starting the thread causes the object's <code>run</code> method to be called in that
	 * separately executing
	 * thread.
	 * <p>
	 * The general contract of the method <code>run</code> is that it may take any action whatsoever.
	 * @see java.lang.Thread#run()
	 */
	@Override
	public void run() {
		while (!m_shutdown) {
			try {
				Thread.sleep(m_sleepInterval * 1000);
			} catch (final InterruptedException e) {
				m_shutdown = true;
				break;
			}

			performStabilization();
			for (int i = 0; i < m_initialNumberOfSuperpeers / 300 || i < 1; i++) {
				fixSuperpeers();
			}

			if (!m_otherSuperpeers.isEmpty()) {
				backupMaintenance();
				m_superpeer.takeOverPeersAndCIDTrees(m_nodeID);
			}
			pingPeers();

			printOverlay();
		}
	}

	/**
	 * Performs stabilization protocol
	 * @note without disappearing superpeers this method does not do anything important; All the setup is done with joining
	 */
	private void performStabilization() {
		while (-1 != m_superpeer.getPredecessor() && m_nodeID != m_superpeer.getPredecessor()) {
			m_logger.trace(getClass(), "Performing stabilization by sending NodeID to predecessor=" + NodeID.toHexString(m_superpeer.getPredecessor()));
			if (m_network.sendMessage(new NotifyAboutNewSuccessorMessage(m_superpeer.getPredecessor(), m_nodeID))
					!= NetworkErrorCodes.SUCCESS) {
				// Predecessor is not available anymore, determine new predecessor and repeat it
				m_superpeer.failureHandling(m_superpeer.getPredecessor());
				continue;
			}
			break;
		}

		while (-1 != m_superpeer.getSuccessor() && m_nodeID != m_superpeer.getSuccessor()) {
			m_logger.trace(getClass(), "Performing stabilization by sending NodeID to successor=" + NodeID.toHexString(m_superpeer.getSuccessor()));
			if (m_network.sendMessage(new NotifyAboutNewPredecessorMessage(m_superpeer.getSuccessor(), m_nodeID))
					!= NetworkErrorCodes.SUCCESS) {
				// Predecessor is not available anymore, determine new predecessor and repeat it
				m_superpeer.failureHandling(m_superpeer.getSuccessor());
				continue;
			}
			break;
		}
	}

	/**
	 * Fixes the superpeer array
	 * @note is called periodically
	 */
	private void fixSuperpeers() {
		boolean stop = false;
		short contactSuperpeer = -1;
		short possibleSuccessor = -1;
		short hisSuccessor;

		AskAboutSuccessorRequest request;
		AskAboutSuccessorResponse response;

		if (1 < m_otherSuperpeers.size()) {
			m_overlayLock.lock();
			if (m_next + 1 < m_otherSuperpeers.size()) {
				contactSuperpeer = m_otherSuperpeers.get(m_next);
				possibleSuccessor = m_otherSuperpeers.get(m_next + 1);
			} else if (m_next + 1 == m_otherSuperpeers.size()) {
				contactSuperpeer = m_otherSuperpeers.get(m_next);
				possibleSuccessor = m_otherSuperpeers.get(0);
			} else {
				m_next = 0;
				fixSuperpeers();
				stop = true;
			}

			if (!stop && contactSuperpeer == m_superpeer.getPredecessor()) {
				m_next++;
				fixSuperpeers();
				stop = true;
			}

			if (!stop) {
				m_next++;
				m_overlayLock.unlock();

				m_logger.trace(getClass(), "Asking " + NodeID.toHexString(contactSuperpeer) + " about his successor to fix overlay");
				request = new AskAboutSuccessorRequest(contactSuperpeer);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// Superpeer is not available anymore, remove from superpeer array and try next superpeer
					m_superpeer.failureHandling(contactSuperpeer);
					m_next--;
					fixSuperpeers();
					return;
				}

				response = request.getResponse(AskAboutSuccessorResponse.class);

				hisSuccessor = response.getSuccessor();

				if (hisSuccessor != possibleSuccessor && -1 != hisSuccessor) {
					m_overlayLock.lock();
					OverlayHelper.insertSuperpeer(hisSuccessor, m_otherSuperpeers);
					m_overlayLock.unlock();
				}
			} else {
				m_overlayLock.unlock();
			}
		}
	}

	/**
	 * Pings all peers and sends current superpeer overlay
	 * @note is called periodically
	 */
	private void pingPeers() {
		short peer;
		int i = 0;

		final ArrayList<Short> peers = m_superpeer.getPeers();
		if (peers != null && peers.size() > 0) {
			while (true) {
				m_overlayLock.lock();
				if (i < peers.size()) {
					peer = peers.get(i++);
					m_overlayLock.unlock();
				} else {
					m_overlayLock.unlock();
					break;
				}
				m_logger.trace(getClass(), "Pinging " + NodeID.toHexString(peer) + " for heartbeat protocol");
				if (m_network.sendMessage(
						new SendSuperpeersMessage(peer, m_otherSuperpeers))
						!= NetworkErrorCodes.SUCCESS) {
					// Peer is not available anymore, remove it from peer array
					m_superpeer.failureHandling(peer);
				}
			}
		}
	}

	/**
	 * Maintain backup replication
	 * @note is called periodically
	 */
	private void backupMaintenance() {
		short[] responsibleArea;

		m_overlayLock.lock();
		responsibleArea = OverlayHelper.getResponsibleArea(m_nodeID, m_superpeer.getPredecessor(), m_otherSuperpeers);
		m_overlayLock.unlock();
		m_logger.trace(getClass(), "Responsible backup area: " + NodeID.toHexString(responsibleArea[0])
				+ ", " + NodeID.toHexString(responsibleArea[1]));

		gatherBackups(responsibleArea);
		m_superpeer.deleteUnnecessaryBackups(responsibleArea);
	}

	/**
	 * Deletes all CIDTrees that are not in the responsible area
	 * @param p_responsibleArea
	 *            the responsible area
	 * @note assumes m_overlayLock has been locked
	 * @note is called periodically
	 */
	private void gatherBackups(final short[] p_responsibleArea) {
		short currentSuperpeer;
		short oldSuperpeer;
		ArrayList<Short> peers;

		AskAboutBackupsRequest request;
		AskAboutBackupsResponse response;

		if (!m_otherSuperpeers.isEmpty()) {
			if (3 >= m_otherSuperpeers.size()) {
				oldSuperpeer = m_nodeID;
				currentSuperpeer = m_superpeer.getSuccessor();
			} else {
				oldSuperpeer = p_responsibleArea[0];
				currentSuperpeer = OverlayHelper.getResponsibleSuperpeer((short) (p_responsibleArea[0] + 1), m_otherSuperpeers, m_overlayLock, m_logger);
			}
			while (-1 != currentSuperpeer) {
				peers = m_superpeer.getPeersInResponsibleArea(oldSuperpeer, currentSuperpeer);

				m_logger.trace(getClass(), "Gathering backups by requesting all backups in responsible area from " + NodeID.toHexString(currentSuperpeer));
				request = new AskAboutBackupsRequest(currentSuperpeer, peers);
				if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
					// CurrentSuperpeer is not available anymore, remove it from superpeer array
					m_superpeer.failureHandling(currentSuperpeer);
					currentSuperpeer = OverlayHelper.getResponsibleSuperpeer((short) (oldSuperpeer + 1), m_otherSuperpeers, m_overlayLock, m_logger);
					peers.clear();
					continue;
				}

				response = request.getResponse(AskAboutBackupsResponse.class);

				m_superpeer.storeIncomingBackups(response.getBackups(), response.getMappings());

				peers.clear();

				if (currentSuperpeer == m_superpeer.getPredecessor()) {
					break;
				}
				oldSuperpeer = currentSuperpeer;
				currentSuperpeer = OverlayHelper.getResponsibleSuperpeer((short) (currentSuperpeer + 1), m_otherSuperpeers, m_overlayLock, m_logger);
			}
		}
	}

	/**
	 * Prints the overlay if something has changed since last call
	 * @note is called periodically
	 */
	private void printOverlay() {
		boolean printed = false;
		short superpeer;
		short peer;
		String superpeersFigure = "Superpeers: ";
		String peersFigure = "Peers: ";

		m_overlayLock.lock();
		for (int i = 0; i < m_otherSuperpeers.size(); i++) {
			superpeer = m_otherSuperpeers.get(i);
			if (!printed && superpeer > m_nodeID) {
				superpeersFigure += " \'" + NodeID.toHexString(m_nodeID) + "\'";
				printed = true;
			}
			superpeersFigure += " " + NodeID.toHexString(superpeer);
		}
		if (!printed) {
			superpeersFigure += " \'" + NodeID.toHexString(m_nodeID) + "\'";
		}

		final ArrayList<Short> peers = m_superpeer.getPeers();
		if (peers != null && peers.size() > 0) {
			for (int i = 0; i < peers.size(); i++) {
				peer = peers.get(i);
				peersFigure += " " + NodeID.toHexString(peer);
			}
		}
		m_overlayLock.unlock();

		if (!(superpeersFigure + peersFigure).equals(m_overlayFigure)) {
			m_logger.info(getClass(), superpeersFigure);
			if (!peersFigure.equals("Peers: ")) {
				m_logger.info(getClass(), peersFigure);
			}
		}
		m_overlayFigure = superpeersFigure + peersFigure;
	}

	/**
	 * Handles an incoming SendBackupsMessage
	 * @param p_sendBackupsMessage
	 *            the SendBackupsMessage
	 */
	private void incomingSendBackupsMessage(final SendBackupsMessage p_sendBackupsMessage) {

		m_logger.trace(getClass(), "Got Message: SEND_BACKUPS_MESSAGE from " + NodeID.toHexString(p_sendBackupsMessage.getSource()));

		m_superpeer.storeIncomingBackups(p_sendBackupsMessage.getCIDTrees(), p_sendBackupsMessage.getMappings());
	}

	/**
	 * Handles an incoming AskAboutBackupsRequest
	 * @param p_askAboutBackupsRequest
	 *            the AskAboutBackupsRequest
	 */
	private void incomingAskAboutBackupsRequest(final AskAboutBackupsRequest p_askAboutBackupsRequest) {
		byte[] allMappings;
		ArrayList<LookupTree> trees;

		m_logger.trace(getClass(), "Got request: ASK_ABOUT_SUCCESSOR_REQUEST from " + NodeID.toHexString(p_askAboutBackupsRequest.getSource()));

		trees = new ArrayList<LookupTree>();
		allMappings = m_superpeer.compareAndReturnBackups(p_askAboutBackupsRequest.getPeers(), trees);

		if (m_network.sendMessage(new AskAboutBackupsResponse(p_askAboutBackupsRequest, trees, allMappings))
				!= NetworkErrorCodes.SUCCESS) {
			// Requesting superpeer is not available anymore, ignore request and remove superpeer
			m_superpeer.failureHandling(p_askAboutBackupsRequest.getSource());
		}
	}

	/**
	 * Handles an incoming AskAboutSuccessorRequest
	 * @param p_askAboutSuccessorRequest
	 *            the AskAboutSuccessorRequest
	 */
	private void incomingAskAboutSuccessorRequest(final AskAboutSuccessorRequest p_askAboutSuccessorRequest) {
		m_logger.trace(getClass(), "Got request: ASK_ABOUT_SUCCESSOR_REQUEST from " + NodeID.toHexString(p_askAboutSuccessorRequest.getSource()));

		if (m_network.sendMessage(
				new AskAboutSuccessorResponse(p_askAboutSuccessorRequest, m_superpeer.getSuccessor()))
				!= NetworkErrorCodes.SUCCESS) {
			// Requesting superpeer is not available anymore, ignore request and remove superpeer
			m_superpeer.failureHandling(p_askAboutSuccessorRequest.getSource());
		}
	}

	/**
	 * Handles an incoming NotifyAboutNewPredecessorMessage
	 * @param p_notifyAboutNewPredecessorMessage
	 *            the NotifyAboutNewPredecessorMessage
	 */
	private void incomingNotifyAboutNewPredecessorMessage(final NotifyAboutNewPredecessorMessage p_notifyAboutNewPredecessorMessage) {
		short possiblePredecessor;

		m_logger.trace(getClass(), "Got Message: NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE from "
				+ NodeID.toHexString(p_notifyAboutNewPredecessorMessage.getSource()));

		possiblePredecessor = p_notifyAboutNewPredecessorMessage.getNewPredecessor();
		if (m_superpeer.getPredecessor() != possiblePredecessor) {
			if (OverlayHelper.isNodeInRange(possiblePredecessor, m_superpeer.getPredecessor(), m_nodeID, OPEN_INTERVAL)) {
				m_overlayLock.lock();
				m_superpeer.setPredecessor(possiblePredecessor);
				m_overlayLock.unlock();
			}
		}
	}

	/**
	 * Handles an incoming NotifyAboutNewSuccessorMessage
	 * @param p_notifyAboutNewSuccessorMessage
	 *            the NotifyAboutNewSuccessorMessage
	 */
	private void incomingNotifyAboutNewSuccessorMessage(final NotifyAboutNewSuccessorMessage p_notifyAboutNewSuccessorMessage) {
		short possibleSuccessor;

		m_logger.trace(getClass(), "Got Message: NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE from " + NodeID.toHexString(p_notifyAboutNewSuccessorMessage.getSource()));

		possibleSuccessor = p_notifyAboutNewSuccessorMessage.getNewSuccessor();
		if (m_superpeer.getSuccessor() != possibleSuccessor) {
			if (OverlayHelper.isNodeInRange(possibleSuccessor, m_nodeID, m_superpeer.getSuccessor(), OPEN_INTERVAL)) {
				m_overlayLock.lock();
				m_superpeer.setSuccessor(possibleSuccessor);
				m_overlayLock.unlock();
			}
		}
	}

	/**
	 * Handles an incoming NotifyAboutFailedPeerMessage
	 * @param p_notifyAboutFailedPeerMessage
	 *            the NotifyAboutFailedPeerMessage
	 */
	private void incomingNotifyAboutFailedPeerMessage(final NotifyAboutFailedPeerMessage p_notifyAboutFailedPeerMessage) {

		m_logger.trace(getClass(), "Got message: NOTIFY_ABOUT_FAILED_PEER_MESSAGE from " + NodeID.toHexString(p_notifyAboutFailedPeerMessage.getSource()));

		m_superpeer.removeFailedPeer(p_notifyAboutFailedPeerMessage.getFailedPeer());
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
				case LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE:
					incomingSendBackupsMessage((SendBackupsMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST:
					incomingAskAboutBackupsRequest((AskAboutBackupsRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST:
					incomingAskAboutSuccessorRequest((AskAboutSuccessorRequest) p_message);
					break;
				case LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE:
					incomingNotifyAboutNewPredecessorMessage((NotifyAboutNewPredecessorMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE:
					incomingNotifyAboutNewSuccessorMessage((NotifyAboutNewSuccessorMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE:
					incomingNotifyAboutFailedPeerMessage((NotifyAboutFailedPeerMessage) p_message);
					break;
				case LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE:
					break;
				default:
					break;
				}
			}
		}
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {
		m_network.register(AskAboutBackupsRequest.class, this);
		m_network.register(AskAboutSuccessorRequest.class, this);
		m_network.register(NotifyAboutNewPredecessorMessage.class, this);
		m_network.register(NotifyAboutNewSuccessorMessage.class, this);
		m_network.register(NotifyAboutFailedPeerMessage.class, this);
	}
}
