
package de.hhu.bsinfo.dxram.failure;

import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.AbstractEvent;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.event.EventListener;
import de.hhu.bsinfo.dxram.failure.events.NodeFailureEvent;
import de.hhu.bsinfo.dxram.failure.messages.FailureMessages;
import de.hhu.bsinfo.dxram.failure.messages.FailureRequest;
import de.hhu.bsinfo.dxram.failure.messages.FailureResponse;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.events.ResponseDelayedEvent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessage;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkException;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles a node failure.
 *
 * @author Kevin Beineke <kevin.beineke@hhu.de> 05.10.16
 */
public class FailureComponent extends AbstractDXRAMComponent implements MessageReceiver, EventListener<AbstractEvent> {

	private static final Logger LOGGER = LogManager.getFormatterLogger(FailureComponent.class.getSimpleName());

	// dependent components
	private AbstractBootComponent m_boot;
	private LookupComponent m_lookup;
	private EventComponent m_event;
	private NetworkComponent m_network;

	private byte[] m_events;
	private ReentrantLock m_eventLock;
	private ReentrantLock m_failureLock;

	/**
	 * Creates the failure component
	 */
	public FailureComponent() {
		super(DXRAMComponentOrder.Init.FAILURE, DXRAMComponentOrder.Shutdown.FAILURE);

		m_events = new byte[Short.MAX_VALUE * 2];
		m_eventLock = new ReentrantLock(false);
		m_failureLock = new ReentrantLock(false);
	}

	/**
	 * Dispatcher for a node failure
	 *
	 * @param p_nodeID NodeID of failed node
	 */
	private void failureHandling(final short p_nodeID) {
		boolean responsible;
		NodeRole ownRole;
		NodeRole roleOfFailedNode;

		// TODO: Do not do failure handling for one node more than once

		m_failureLock.lock();

		ownRole = m_boot.getNodeRole();
		roleOfFailedNode = m_boot.getNodeRole(p_nodeID);

		if (ownRole == NodeRole.SUPERPEER) {
			// #if LOGGER >= DEBUG
			LOGGER.debug("********** ********** Node Failure ********** **********");
			// #endif /* LOGGER >= DEBUG */

			// Restore superpeer overlay and/or initiate recovery
			responsible = m_lookup.failureHandling(p_nodeID, roleOfFailedNode);

			if (responsible) {
				// Failed node was either the predecessor superpeer or a peer/terminal this superpeer is responsible for

				// #if LOGGER >= DEBUG
				LOGGER.debug("Failed node was a %s, NodeID: 0x%X", roleOfFailedNode, p_nodeID);
				// #endif /* LOGGER >= DEBUG */

				// Clean-up zookeeper
				m_boot.failureHandling(p_nodeID, roleOfFailedNode);
			} else {
				// #if LOGGER >= DEBUG
				LOGGER.debug("Not responsible for failed node, NodeID: 0x%X", p_nodeID);
				// #endif /* LOGGER >= DEBUG */
			}
		} else {
			// This is a peer or terminal
			if (roleOfFailedNode == NodeRole.PEER) {
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
				LOGGER.debug("ConnectionLostEvent triggered: 0x%X", nodeID);
				// #endif /* LOGGER == DEBUG */

				try {
					m_network.connectNode(nodeID);

					// #if LOGGER == DEBUG
					LOGGER.debug("Re-connect successful, continuing");
					// #endif /* LOGGER == DEBUG */
				} catch (final NetworkException e) {
					// #if LOGGER == DEBUG
					LOGGER.debug("Node is unreachable. Initiating failure handling");
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
				LOGGER.debug("ResponseDelayedEvent triggered: 0x%X", nodeID);
				// #endif /* LOGGER == DEBUG */

				try {
					m_network.sendMessage(new DefaultMessage(nodeID));

					// #if LOGGER == DEBUG
					LOGGER.debug("Node is still reachable, continuing");
					// #endif /* LOGGER == DEBUG */
				} catch (final NetworkException e) {
					// #if LOGGER == DEBUG
					LOGGER.debug("Node is unreachable. Initiating failure handling");
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
	 *
	 * @param p_request the FailureRequest
	 */
	private void incomingFailureRequest(final FailureRequest p_request) {
		// Outsource failure handling to another thread to avoid blocking a message handler
		Runnable task = () -> {
			failureHandling(p_request.getFailedNode());
		};
		new Thread(task).start();

		try {
			m_network.sendMessage(new FailureResponse(p_request));
		} catch (final NetworkException e) {

		}
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
	protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
		m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
		m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
		m_network = p_componentAccessor.getComponent(NetworkComponent.class);
		m_event = p_componentAccessor.getComponent(EventComponent.class);
	}

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_network.registerMessageType(DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_REQUEST,
				FailureRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.FAILURE_MESSAGES_TYPE, FailureMessages.SUBTYPE_FAILURE_RESPONSE,
				FailureResponse.class);
		m_network.register(FailureRequest.class, this);

		m_event.registerListener(this, ConnectionLostEvent.class);
		m_event.registerListener(this, ResponseDelayedEvent.class);

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		return true;
	}

}
