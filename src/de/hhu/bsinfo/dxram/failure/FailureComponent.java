
package de.hhu.bsinfo.dxram.failure;

import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.failure.messages.FailureMessages;
import de.hhu.bsinfo.dxram.failure.messages.FailureRequest;
import de.hhu.bsinfo.dxram.failure.messages.FailureResponse;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.events.ResponseDelayedEvent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessage;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Handles a node failure.
 * @author Kevin Beineke <kevin.beineke@hhu.de> 05.10.16
 */
public class FailureComponent extends AbstractDXRAMComponent implements MessageReceiver, EventListener<AbstractEvent> {

	private AbstractBootComponent m_boot;
	private LoggerComponent m_logger;
	private LookupComponent m_lookup;
	private EventComponent m_event;

	private NetworkComponent m_network;

	private byte[] m_events;
	private ReentrantLock m_eventLock;
	private ReentrantLock m_failureLock;

	/**
	 * Creates the failure component
	 * @param p_priorityInit
	 *            the initialization priority
	 * @param p_priorityShutdown
	 *            the shutdown priority
	 */
	public FailureComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);

		m_events = new byte[Short.MAX_VALUE * 2];
		m_eventLock = new ReentrantLock(false);
		m_failureLock = new ReentrantLock(false);
	}

	/**
	 * Dispatcher for a node failure
	 * @param p_nodeID
	 *            NodeID of failed node
	 */
	private void failureHandling(final short p_nodeID) {
		NodeRole ownRole;
		NodeRole roleOfFailedNode;

		// TODO: Do not do failure handling for one node more than once

		m_failureLock.lock();

		ownRole = m_boot.getNodeRole();
		roleOfFailedNode = m_boot.getNodeRole(p_nodeID);

		if (ownRole == NodeRole.SUPERPEER) {
			// #if LOGGER >= DEBUG
			m_logger.debug(getClass(), "********** ********** Node Failure ********** **********");
			// #endif /* LOGGER >= DEBUG */

			if (m_lookup.isResponsibleForFailureHandling(p_nodeID)) {
				// Failed node was either a superpeer or a peer/terminal this superpeer is responsible for

				// #if LOGGER >= DEBUG
				m_logger.debug(getClass(),
						"Failed node was a " + roleOfFailedNode + ", NodeID: " + NodeID.toHexString(p_nodeID));
				// #endif /* LOGGER >= DEBUG */

				// Restore superpeer overlay and/or initiate recovery
				m_lookup.failureHandling(p_nodeID, roleOfFailedNode);

				// Clean-up zookeeper
				m_boot.failureHandling(p_nodeID, roleOfFailedNode);
			} else {
				// #if LOGGER >= DEBUG
				m_logger.debug(getClass(),
						"Not responsible for failed node, NodeID: " + NodeID.toHexString(p_nodeID));
				// #endif /* LOGGER >= DEBUG */
			}
		} else {
			// This is a peer or terminal
			if (roleOfFailedNode == NodeRole.PEER) {

				// TODO: Inform BackupComponent to replace failed peer with online backup peers and to log
				// TODO: corresponding chunks

				// Notify other components/services (PeerLockService, ZooKeeperBootComponent, LookupComponent)
				m_event.fireEvent(new NodeFailureEvent(getClass().getSimpleName(), p_nodeID, roleOfFailedNode));
			}
		}

		m_failureLock.unlock();
	}

	@Override
	public void eventTriggered(final AbstractEvent p_event) {
		if (p_event instanceof ConnectionLostEvent) {
			ConnectionLostEvent event = (ConnectionLostEvent) p_event;
			short nodeID = event.getNodeID();

			m_eventLock.lock();
			if (m_events[nodeID & 0xFFFF] == 0) {
				m_events[nodeID & 0xFFFF] = 1;
				m_eventLock.unlock();

				// #if LOGGER == DEBUG
				m_logger.debug(getClass(), "ConnectionLostEvent triggered: " + NodeID.toHexString(nodeID));
				// #endif /* LOGGER == DEBUG */

				if (m_network.connectNode(nodeID) == NetworkErrorCodes.SUCCESS) {
					// #if LOGGER == DEBUG
					m_logger.debug(getClass(), "Re-connect successful, continuing.");
					// #endif /* LOGGER == DEBUG */
				} else {
					// #if LOGGER == DEBUG
					m_logger.debug(getClass(), "Node is unreachable. Initiating failure handling.");
					// #endif /* LOGGER == DEBUG */

					failureHandling(nodeID);
				}

				m_eventLock.lock();
				m_events[nodeID & 0xFFFF] = 0;
			}
			m_eventLock.unlock();
		} else {
			ResponseDelayedEvent event = (ResponseDelayedEvent) p_event;
			short nodeID = event.getNodeID();

			m_eventLock.lock();
			if (m_events[nodeID & 0xFFFF] == 0) {
				m_events[nodeID & 0xFFFF] = 1;
				m_eventLock.unlock();

				// #if LOGGER == DEBUG
				m_logger.debug(getClass(), "ResponseDelayedEvent triggered: " + NodeID.toHexString(nodeID));
				// #endif /* LOGGER == DEBUG */

				if (m_network.sendMessage(new DefaultMessage((short) nodeID)) == NetworkErrorCodes.SUCCESS) {
					// #if LOGGER == DEBUG
					m_logger.debug(getClass(), "Node is still reachable, continuing.");
					// #endif /* LOGGER == DEBUG */
				} else {
					// #if LOGGER == DEBUG
					m_logger.debug(getClass(), "Node is unreachable. Initiating failure handling.");
					// #endif /* LOGGER == DEBUG */

					failureHandling(nodeID);
				}

				m_eventLock.lock();
				m_events[nodeID & 0xFFFF] = 0;
			}
			m_eventLock.unlock();
		}
	}

	/**
	 * Handles an incoming FailureRequest
	 * @param p_request
	 *            the FailureRequest
	 */
	private void incomingFailureRequest(final FailureRequest p_request) {
		// Outsource failure handling to another thread to avoid blocking a message handler
		Runnable task = () -> {
			failureHandling(p_request.getFailedNode());
		};
		new Thread(task).start();

		m_network.sendMessage(new FailureResponse(p_request));
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {

		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.FAILURE_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case FailureMessages.SUBTYPE_FAILURE_REQUEST:
						incomingFailureRequest((FailureRequest) p_message);
						break;
					default:
						break;
				}
			}
		}
	}

	// --------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
		m_boot = getDependentComponent(AbstractBootComponent.class);
		m_logger = getDependentComponent(LoggerComponent.class);
		m_lookup = getDependentComponent(LookupComponent.class);

		m_network = getDependentComponent(NetworkComponent.class);
		m_network.registerMessageType(DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_REQUEST,
				FailureRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_RESPONSE,
				FailureResponse.class);
		m_network.register(FailureRequest.class, this);

		m_event = getDependentComponent(EventComponent.class);
		m_event.registerListener(this, ConnectionLostEvent.class);
		m_event.registerListener(this, ResponseDelayedEvent.class);

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
