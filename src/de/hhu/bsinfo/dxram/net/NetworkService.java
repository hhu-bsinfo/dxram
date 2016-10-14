
package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

/**
 * Service to access the backend network service for sending messages
 * to other participating nodes in the system.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NetworkService extends AbstractDXRAMService {

	private NetworkComponent m_network;

	/**
	 * Constructor
	 */
	public NetworkService() {
		super("net");
	}

	/**
	 * Registers a message type
	 *
	 * @param p_type    the unique type
	 * @param p_subtype the unique subtype
	 * @param p_class   the calling class
	 */
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		m_network.registerMessageType(p_type, p_subtype, p_class);
	}

	/**
	 * Registers a message receiver
	 *
	 * @param p_message  the message
	 * @param p_receiver the receiver
	 */
	public void registerReceiver(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_network.register(p_message, p_receiver);
	}

	/**
	 * Unregisters a message receiver
	 *
	 * @param p_message  the message
	 * @param p_receiver the receiver
	 */
	public void unregisterReceiver(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		m_network.unregister(p_message, p_receiver);
	}

	/**
	 * Send a message.
	 *
	 * @param p_message Message to send
	 * @return NetworkErrorCode, refer to enum
	 */
	public NetworkErrorCodes sendMessage(final AbstractMessage p_message) {
		return m_network.sendMessage(p_message);
	}

	/**
	 * Send the Request and wait for fulfillment (wait for response).
	 *
	 * @param p_request The request to send.
	 * @return 0 if successful, -1 if sending the request failed, 1 waiting for the response timed out.
	 */
	public NetworkErrorCodes sendSync(final AbstractRequest p_request) {
		return m_network.sendSync(p_request);
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_network = getComponent(NetworkComponent.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_network = null;

		return true;
	}

}
