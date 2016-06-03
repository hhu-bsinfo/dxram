
package de.hhu.bsinfo.dxram.net;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.NetworkHandler;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.RequestMap;

/**
 * Access to the network interface to send messages or requests
 * to other nodes.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NetworkComponent extends AbstractDXRAMComponent {

	private LoggerComponent m_logger;
	private AbstractBootComponent m_boot;

	// Attributes
	private NetworkHandler m_networkHandler;
	private int m_requestTimeoutMs = -1;

	/**
	 * Constructor
	 * @param p_priorityInit
	 *            Priority for initialization of this component.
	 *            When choosing the order, consider component dependencies here.
	 * @param p_priorityShutdown
	 *            Priority for shutting down this component.
	 *            When choosing the order, consider component dependencies here.
	 */
	public NetworkComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
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
	 * @param p_type
	 *            the unique type
	 * @param p_subtype
	 *            the unique subtype
	 * @param p_class
	 *            the calling class
	 */
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
	}

	/**
	 * Send a message.
	 * @param p_message
	 *            Message to send
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
			break;
		case -2:
			errCode = NetworkErrorCodes.SEND_DATA;
			break;
		default:
			assert 1 == 2;
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
	 * @param p_request
	 *            The request to send.
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
				m_logger.error(this.getClass(), "Sending sync, waiting for responses " + p_request + " failed, timeout.");
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
	 * @param p_message
	 *            the message
	 * @param p_receiver
	 *            the receiver
	 */
	public void register(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_networkHandler.register(p_message, p_receiver);
	}

	/**
	 * Unregisters a message receiver
	 * @param p_message
	 *            the message
	 * @param p_receiver
	 *            the receiver
	 */
	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_networkHandler.unregister(p_message, p_receiver);
	}

	// --------------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_HANDLER);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.REQUEST_MAP_ENTRY_COUNT);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.INCOMING_BUFFER_SIZE);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.OUTGOING_BUFFER_SIZE);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.NUMBER_OF_BUFFERS);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.FLOW_CONTROL_WINDOW_SIZE);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.REQUEST_TIMEOUT_MS);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.CONNECTION_TIMEOUT_MS);
	}

	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) {
		m_logger = getDependentComponent(LoggerComponent.class);
		m_boot = getDependentComponent(AbstractBootComponent.class);

		m_networkHandler = new NetworkHandler(
				p_settings.getValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_HANDLER),
				p_settings.getValue(NetworkConfigurationValues.Component.REQUEST_MAP_ENTRY_COUNT));

		m_networkHandler.setLogger(m_logger);

		// Check if given ip address is bound to one of this node's network interfaces
		boolean found = false;
		InetAddress myAddress = m_boot.getNodeAddress(m_boot.getNodeID()).getAddress();
		try {
			Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
			outerloop: while (networkInterfaces.hasMoreElements()) {
				NetworkInterface currentNetworkInterface = (NetworkInterface) networkInterfaces.nextElement();
				Enumeration<InetAddress> addresses = currentNetworkInterface.getInetAddresses();
				while (addresses.hasMoreElements()) {
					InetAddress currentAddress = (InetAddress) addresses.nextElement();
					if (myAddress.equals(currentAddress)) {
						// #if LOGGER >= INFO
						m_logger.info(getClass(), myAddress.getHostAddress() + " is bound to " + currentNetworkInterface.getDisplayName());
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
				m_logger.error(getClass(), "Could not find network interface with address " + myAddress.getHostAddress());
				// #endif /* LOGGER >= ERROR */
				return false;
			}
		}

		m_networkHandler.initialize(
				m_boot.getNodeID(),
				new NodeMappings(m_boot),
				p_settings.getValue(NetworkConfigurationValues.Component.INCOMING_BUFFER_SIZE),
				p_settings.getValue(NetworkConfigurationValues.Component.OUTGOING_BUFFER_SIZE),
				p_settings.getValue(NetworkConfigurationValues.Component.NUMBER_OF_BUFFERS),
				p_settings.getValue(NetworkConfigurationValues.Component.FLOW_CONTROL_WINDOW_SIZE),
				p_settings.getValue(NetworkConfigurationValues.Component.CONNECTION_TIMEOUT_MS));

		m_requestTimeoutMs = p_settings.getValue(NetworkConfigurationValues.Component.REQUEST_TIMEOUT_MS);

		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_networkHandler.close();

		m_networkHandler = null;

		return true;
	}
}
