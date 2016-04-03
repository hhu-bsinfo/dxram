
package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.NetworkHandler;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

/**
 * Access to the network interface to send messages or requests
 * to other nodes.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NetworkComponent extends DXRAMComponent {

	private LoggerComponent m_logger = null;
	private BootComponent m_boot = null;

	// Attributes
	private NetworkHandler m_networkHandler = null;
	private int m_requestTimeoutMs = -1;

	public NetworkComponent(final int p_priorityInit, final int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
	}

	// --------------------------------------------------------------------------------------

	public void activateConnectionManager() {
		m_networkHandler.activateConnectionManager();
	}

	public void deactivateConnectionManager() {
		m_networkHandler.deactivateConnectionManager();
	}

	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
	}

	public NetworkErrorCodes sendMessage(final AbstractMessage p_message) {
		m_logger.trace(getClass(), "Sending message " + p_message);
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

		if (errCode != NetworkErrorCodes.SUCCESS) {
			m_logger.error(this.getClass(), "Sending message " + p_message + " failed: " + errCode);
		}

		return errCode;
	}

	/**
	 * Send the Request and wait for fulfillment (wait for response).
	 * @param p_request
	 *            The request to send.
	 * @return 0 if successful, -1 if sending the request failed, 1 waiting for the response timed out.
	 */
	public NetworkErrorCodes sendSync(final AbstractRequest p_request) {
		m_logger.trace(getClass(), "Sending request (sync): " + p_request);
		NetworkErrorCodes err = sendMessage(p_request);
		if (err == NetworkErrorCodes.SUCCESS) {
			m_logger.trace(getClass(), "Waiting for response to request: " + p_request);
			if (!p_request.waitForResponses(m_requestTimeoutMs)) {
				m_logger.error(this.getClass(), "Sending sync, waiting for responses " + p_request + " failed, timeout.");
				err = NetworkErrorCodes.RESPONSE_TIMEOUT;
			} else {
				m_logger.trace(getClass(), "Received response: " + p_request.getResponse());
			}
		}

		return err;
	}

	public void register(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_networkHandler.register(p_message, p_receiver);
	}

	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_networkHandler.unregister(p_message, p_receiver);
	}

	// --------------------------------------------------------------------------------------

	@Override
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_HANDLER);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_CREATOR);
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
		m_boot = getDependentComponent(BootComponent.class);

		m_networkHandler = new NetworkHandler(
				p_settings.getValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_CREATOR),
				p_settings.getValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_HANDLER));

		m_networkHandler.setLogger(m_logger);
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
