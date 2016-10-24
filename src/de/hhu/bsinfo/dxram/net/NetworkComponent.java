
package de.hhu.bsinfo.dxram.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import com.google.gson.annotations.Expose;
import de.hhu.bsinfo.dxram.DXRAMComponentOrder;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.event.EventComponent;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.net.events.ConnectionLostEvent;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessage;
import de.hhu.bsinfo.dxram.net.messages.DefaultMessages;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.AbstractRequest;
import de.hhu.bsinfo.ethnet.NetworkHandler;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.ethnet.NodeID;
import de.hhu.bsinfo.ethnet.RequestMap;
import de.hhu.bsinfo.utils.StorageUnit;

/**
 * Access to the network interface to send messages or requests
 * to other nodes.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NetworkComponent extends AbstractDXRAMComponent {

	// configuration values
	@Expose
	private int m_threadCountMsgHandler = 1;
	@Expose
	private int m_requestMapEntryCount = (int) Math.pow(2, 20);
	@Expose
    private StorageUnit m_incomingBufferSize = new StorageUnit(1, StorageUnit.MB);
	@Expose
    private StorageUnit m_outgoingBufferSize = new StorageUnit(1, StorageUnit.MB);
	@Expose
	private int m_numberOfPendingBuffersPerConnection = 100;
	@Expose
    private StorageUnit m_flowControlWindowSize = new StorageUnit(1, StorageUnit.MB);
	@Expose
	private int m_requestTimeoutMs = 333;

	// dependent components
	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;
	private EventComponent m_event;

	// Attributes
	private NetworkHandler m_networkHandler;

	/**
	 * Constructor
	 */
	public NetworkComponent() {
		super(DXRAMComponentOrder.Init.NETWORK, DXRAMComponentOrder.Shutdown.NETWORK);
	}

	// --------------------------------------------------------------------------------------

	/**
	 * Activates the connection manager
	 */
	public void activateConnectionManager() {
		m_networkHandler.activateConnectionManager();
	}

	/**
	 * Deactivates the connection manager
	 */
	public void deactivateConnectionManager() {
		m_networkHandler.deactivateConnectionManager();
	}

	/**
	 * Registers a message type
	 *
	 * @param p_type    the unique type
	 * @param p_subtype the unique subtype
	 * @param p_class   the calling class
	 */
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
	}

	/**
	 * Connect a node.
	 *
	 * @param p_nodeID Node to connect
	 * @return 0 if successful, -1 if not
	 */
	public NetworkErrorCodes connectNode(final short p_nodeID) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Connecting node " + NodeID.toHexString(p_nodeID));
		// #endif /* LOGGER == TRACE */

		int res = m_networkHandler.connectNode(p_nodeID);
		NetworkErrorCodes errCode = NetworkErrorCodes.SUCCESS;
		if (res == -1) {
			errCode = NetworkErrorCodes.DESTINATION_UNREACHABLE;

			// #if LOGGER >= ERROR
			m_logger.error(this.getClass(), "Connecting node " + NodeID.toHexString(p_nodeID) + " failed: " + errCode);
			// #endif /* LOGGER >= ERROR */
		}

		return errCode;
	}

	/**
	 * Send a message.
	 *
	 * @param p_message Message to send
	 * @return NetworkErrorCode, refer to enum
	 */
	public NetworkErrorCodes sendMessage(final AbstractMessage p_message) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Sending message " + p_message);
		// #endif /* LOGGER == TRACE */

		int res = m_networkHandler.sendMessage(p_message);
		NetworkErrorCodes errCode = NetworkErrorCodes.UNKNOWN;

		switch (res) {
			case 0:
				errCode = NetworkErrorCodes.SUCCESS;
				break;
			case -1:
				errCode = NetworkErrorCodes.DESTINATION_UNREACHABLE;

				// Connection creation failed -> trigger failure handling
				m_event.fireEvent(new ConnectionLostEvent(getClass().getSimpleName(), p_message.getDestination()));
				break;
			case -2:
				errCode = NetworkErrorCodes.SEND_DATA;
				break;
			default:
				assert false;
				break;
		}

		// #if LOGGER >= ERROR
		if (errCode != NetworkErrorCodes.SUCCESS) {
			m_logger.error(this.getClass(), "Sending message " + p_message + " failed: " + errCode);
		}
		// #endif /* LOGGER >= ERROR */

		return errCode;
	}

	/**
	 * Send the Request and wait for fulfillment (wait for response).
	 *
	 * @param p_request The request to send.
	 * @return 0 if successful, -1 if sending the request failed, 1 waiting for the response timed out.
	 */
	public NetworkErrorCodes sendSync(final AbstractRequest p_request) {
		// #if LOGGER == TRACE
		m_logger.trace(getClass(), "Sending request (sync): " + p_request);
		// #endif /* LOGGER == TRACE */

		NetworkErrorCodes err = sendMessage(p_request);
		if (err == NetworkErrorCodes.SUCCESS) {
			// #if LOGGER == TRACE
			m_logger.trace(getClass(), "Waiting for response to request: " + p_request);
			// #endif /* LOGGER == TRACE */

			if (!p_request.waitForResponses(m_requestTimeoutMs)) {
				// #if LOGGER >= ERROR
				m_logger.error(this.getClass(),
						"Sending sync, waiting for responses " + p_request + " failed, timeout.");
				// #endif /* LOGGER >= ERROR */

				// #if LOGGER >= DEBUG
				m_logger.debug(this.getClass(), m_networkHandler.getStatus());
				// #endif /* LOGGER >= DEBUG */

				err = NetworkErrorCodes.RESPONSE_TIMEOUT;
			} else {
				// #if LOGGER == TRACE
				m_logger.trace(getClass(), "Received response: " + p_request.getResponse());
				// #endif /* LOGGER == TRACE */
			}
		}

		if (err != NetworkErrorCodes.SUCCESS) {
			RequestMap.remove(p_request.getRequestID());
		}

		return err;
	}

	/**
	 * Registers a message receiver
	 *
	 * @param p_message  the message
	 * @param p_receiver the receiver
	 */
	public void register(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_networkHandler.register(p_message, p_receiver);
	}

	/**
	 * Unregisters a message receiver
	 *
	 * @param p_message  the message
	 * @param p_receiver the receiver
	 */
	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_networkHandler.unregister(p_message, p_receiver);
	}

	// --------------------------------------------------------------------------------------

	@Override
	protected boolean initComponent(final DXRAMContext.EngineSettings p_engineEngineSettings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		m_boot = getDependentComponent(AbstractBootComponent.class);
		m_event = getDependentComponent(EventComponent.class);

		m_networkHandler = new NetworkHandler(m_threadCountMsgHandler, m_requestMapEntryCount);
		m_networkHandler.setLogger(m_logger);
		m_networkHandler.setEventHandler(getDependentComponent(EventComponent.class));

		// Check if given ip address is bound to one of this node's network interfaces
		boolean found = false;
		InetAddress myAddress = m_boot.getNodeAddress(m_boot.getNodeID()).getAddress();
		try {
			Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
			outerloop:
			while (networkInterfaces.hasMoreElements()) {
				NetworkInterface currentNetworkInterface = (NetworkInterface) networkInterfaces.nextElement();
				Enumeration<InetAddress> addresses = currentNetworkInterface.getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress currentAddress = (InetAddress) addresses.nextElement();
					if (myAddress.equals(currentAddress)) {
						// #if LOGGER >= INFO
						m_logger.info(getClass(), myAddress.getHostAddress() + " is bound to " + currentNetworkInterface
								.getDisplayName());
						// #endif /* LOGGER >= INFO */
						found = true;
						break outerloop;
					}
				}
			}
		} catch (final SocketException e1) {
			// #if LOGGER >= ERROR
			m_logger.error(getClass(), "Could not get network interfaces for ip confirmation");
			// #endif /* LOGGER >= ERROR */
		} finally {
			if (!found) {
				// #if LOGGER >= ERROR
				m_logger.error(getClass(),
						"Could not find network interface with address " + myAddress.getHostAddress());
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		}

		m_networkHandler.initialize(
				m_boot.getNodeID(),
				new NodeMappings(m_boot),
                (int) m_incomingBufferSize.getBytes(),
                (int) m_outgoingBufferSize.getBytes(),
				m_numberOfPendingBuffersPerConnection,
                (int) m_flowControlWindowSize.getBytes(),
				m_requestTimeoutMs);

		m_networkHandler.registerMessageType(DXRAMMessageTypes.DEFAULT_MESSAGES_TYPE,
				DefaultMessages.SUBTYPE_DEFAULT_MESSAGE, DefaultMessage.class);

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_networkHandler.close();

		m_networkHandler = null;

		return true;
	}
}
