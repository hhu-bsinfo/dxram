package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMComponent;
import de.hhu.bsinfo.dxram.engine.DXRAMEngine;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.NetworkHandler;
import de.hhu.bsinfo.menet.NetworkInterface.MessageReceiver;

/**
 * 
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class NetworkComponent extends DXRAMComponent {
		
	public enum ErrorCode 
	{
		SUCCESS,
		UNKNOWN,
		DESTINATION_UNREACHABLE,
		SEND_DATA,
		RESPONSE_TIMEOUT,
	}
	
	private LoggerComponent m_logger = null;
	private BootComponent m_boot = null;
	
	// Attributes
	private NetworkHandler m_networkHandler;
	
	public NetworkComponent(int p_priorityInit, int p_priorityShutdown) {
		super(p_priorityInit, p_priorityShutdown);
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
		m_logger.trace(getClass(), "Sending message " + p_message);
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
			m_logger.error(this.getClass(), "Sending message " + p_message + " failed: " + errCode);
		}
		
		return errCode;
	}
	
	
	public ErrorCode forwardMessage(final short p_destination, final AbstractMessage p_message) {
		m_logger.trace(getClass(), "Forwarding message " + p_message);
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
			m_logger.error(this.getClass(), "Forwarding message " + p_message + " failed: " + errCode);
		}
		
		return errCode;
	}
	
	/**
	 * Send the Request and wait for fulfillment (wait for response).
	 * @param p_request The request to send.
	 * @return 0 if successful, -1 if sending the request failed, 1 waiting for the response timed out.
	 */
	public ErrorCode sendSync(final AbstractRequest p_request) {
		m_logger.trace(getClass(), "Sending request (sync): " + p_request);
		ErrorCode err = sendMessage(p_request);
		if (err == ErrorCode.SUCCESS) {
			m_logger.trace(getClass(), "Waiting for response to request: " + p_request);
			if (!p_request.waitForResponses()) {
				m_logger.error(this.getClass(), "Sending sync, waiting for responses " + p_request + " failed, timeout.");
				err = ErrorCode.RESPONSE_TIMEOUT;
			} else {		
				m_logger.trace(getClass(), "Received response: " + p_request.getResponse());
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
	protected void registerDefaultSettingsComponent(final Settings p_settings) {
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.MSG_BUFFER_SIZE);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_HANDLER);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.THREAD_COUNT_TASK_HANDLER);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.STATISTICS_THROUGHPUT);
		p_settings.setDefaultValue(NetworkConfigurationValues.Component.STATISTICS_REQUESTS);
	}
	
	@Override
	protected boolean initComponent(final DXRAMEngine.Settings p_engineSettings, final Settings p_settings) 
	{
		m_logger = getDependentComponent(LoggerComponent.class);
		m_boot = getDependentComponent(BootComponent.class);
		
		m_networkHandler = new NetworkHandler(
				p_settings.getValue(NetworkConfigurationValues.Component.THREAD_COUNT_TASK_HANDLER),
				p_settings.getValue(NetworkConfigurationValues.Component.THREAD_COUNT_MSG_HANDLER),
				p_settings.getValue(NetworkConfigurationValues.Component.STATISTICS_THROUGHPUT),
				p_settings.getValue(NetworkConfigurationValues.Component.STATISTICS_REQUESTS));
		
		m_networkHandler.setLogger(m_logger);
		
		m_networkHandler.initialize(
				m_boot.getNodeID(), 
				new NodeMappings(m_boot), 
				p_settings.getValue(NetworkConfigurationValues.Component.MSG_BUFFER_SIZE));
		
		return true;
	}

	@Override
	protected boolean shutdownComponent() {
		m_networkHandler.close();
		
		m_networkHandler = null;
		
		return true;
	}
}
