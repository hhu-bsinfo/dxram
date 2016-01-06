package de.uniduesseldorf.dxram.core.net;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.AbstractRequest;
import de.uniduesseldorf.menet.NetworkHandler;
import de.uniduesseldorf.menet.NetworkInterface.MessageReceiver;
import de.uniduesseldorf.utils.config.Configuration;

public class NetworkComponent extends DXRAMComponent {
	
	public static final String COMPONENT_IDENTIFIER = "Network";
	
	private final Logger LOGGER = Logger.getLogger(NetworkComponent.class);
	
	public enum ErrorCode 
	{
		SUCCESS,
		UNKNOWN,
		DESTINATION_UNREACHABLE,
		SEND_DATA,
		RESPONSE_TIMEOUT,
	}
	
	// Attributes
	private NetworkHandler m_networkHandler;
	
	public NetworkComponent(int p_priorityInit, int p_priorityShutdown) {
		super("Network", p_priorityInit, p_priorityShutdown);
	}

	// --------------------------------------------------------------------------------------

	public void activateConnectionManager() {
		m_networkHandler.activateConnectionManager();
	}

	public void deactivateConnectionManager() {
		m_networkHandler.deactivateConnectionManager();
	}
	
	public void registerMessageType(byte p_type, byte p_subtype, Class<?> p_class) {
		m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
	}

	public ErrorCode sendMessage(final AbstractMessage p_message) {
		int res = m_networkHandler.sendMessage(p_message);
		ErrorCode errCode = ErrorCode.UNKNOWN;
		
		switch (res) {
			case 0:
				errCode = ErrorCode.SUCCESS; break;
			case -1:
				errCode = ErrorCode.DESTINATION_UNREACHABLE; break;
			case -2:
				errCode = ErrorCode.SEND_DATA; break;
			default:
				assert 1 == 2; break;
		}
		
		if (errCode != ErrorCode.SUCCESS) {
			LOGGER.error("Sending message " + p_message + " failed: " + errCode);
		}
		
		return errCode;
	}
	
	
	public ErrorCode forwardMessage(final short p_destination, final AbstractMessage p_message) {
		int res = m_networkHandler.forwardMessage(p_destination, p_message);
		
		ErrorCode errCode = ErrorCode.UNKNOWN;
		
		switch (res) {
			case 0:
				errCode = ErrorCode.SUCCESS; break;
			case -1:
				errCode = ErrorCode.DESTINATION_UNREACHABLE; break;
			case -2:
				errCode = ErrorCode.SEND_DATA; break;
			default:
				assert 1 == 2; break;
		}
		
		if (errCode != ErrorCode.SUCCESS) {
			LOGGER.error("Forwarding message " + p_message + " failed: " + errCode);
		}
		
		return errCode;
	}
	
	/**
	 * Send the Request and wait for fulfillment (wait for response).
	 * @param p_request The request to send.
	 * @return 0 if successful, -1 if sending the request failed, 1 waiting for the response timed out.
	 */
	public ErrorCode sendSync(final AbstractRequest p_request) {
		ErrorCode err = sendMessage(p_request);
		if (err == ErrorCode.SUCCESS) {
			if (!p_request.waitForResponses()) {
				LOGGER.error("Sending sync, waiting for responses " + p_request + " failed, timeout.");
				err = ErrorCode.RESPONSE_TIMEOUT;
			}		
		}
		
		return err;
	}

	public void register(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver) {
		m_networkHandler.register(p_message, p_receiver);
	}

	public void unregister(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver) {
		m_networkHandler.unregister(p_message, p_receiver);
	}
	
	// --------------------------------------------------------------------------------------

	@Override
	protected void registerConfigurationValuesComponent(final Configuration p_configuration) {
		p_configuration.registerConfigurationEntries(NetworkConfigurationValues.CONFIGURATION_ENTRIES);
	}
	
	@Override
	protected boolean initComponent(final Configuration p_configuration) 
	{
		m_networkHandler = new NetworkHandler(
				p_configuration.getIntValue(NetworkConfigurationValues.NETWORK_TASK_HANDLER_THREAD_COUNT),
				p_configuration.getIntValue(NetworkConfigurationValues.NETWORK_MESSAGE_HANDLER_THREAD_COUNT),
				p_configuration.getBooleanValue(NetworkConfigurationValues.NETWORK_STATISTICS_THROUGHPUT),
				p_configuration.getBooleanValue(NetworkConfigurationValues.NETWORK_STATISTICS_REQUESTS));
		
		m_networkHandler.initialize(
				getSystemData().getNodeID(), 
				new NodeMappings(getSystemData()), 
				p_configuration.getIntValue(NetworkConfigurationValues.NETWORK_BUFFER_SIZE));
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_networkHandler.close();
		
		m_networkHandler = null;
		
		return true;
	}
}
