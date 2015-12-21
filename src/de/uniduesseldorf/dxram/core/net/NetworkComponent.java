package de.uniduesseldorf.dxram.core.net;

import de.uniduesseldorf.dxram.core.engine.DXRAMComponent;
import de.uniduesseldorf.dxram.core.engine.config.DXRAMConfigurationConstants;

import de.uniduesseldorf.menet.AbstractMessage;
import de.uniduesseldorf.menet.NetworkException;
import de.uniduesseldorf.menet.NetworkHandler;
import de.uniduesseldorf.menet.NetworkInterface;
import de.uniduesseldorf.utils.config.Configuration;

public class NetworkComponent extends DXRAMComponent implements NetworkInterface {

	public static final String COMPONENT_IDENTIFIER = "Network";
	
	// Attributes
	private NetworkHandler m_networkHandler;
	
	public NetworkComponent(int p_priorityInit, int p_priorityShutdown) {
		super("Network", p_priorityInit, p_priorityShutdown);
	}

	// --------------------------------------------------------------------------------------
	
	@Override
	public void activateConnectionManager() {
		m_networkHandler.activateConnectionManager();
	}

	@Override
	public void deactivateConnectionManager() {
		m_networkHandler.deactivateConnectionManager();
	}

	@Override
	public void registerMessageType(byte p_type, byte p_subtype, Class<?> p_class) {
		m_networkHandler.registerMessageType(p_type, p_subtype, p_class);
	}

	@Override
	public void sendMessage(AbstractMessage p_message) throws NetworkException {
		m_networkHandler.sendMessage(p_message);
	}

	@Override
	public void register(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver) {
		m_networkHandler.register(p_message, p_receiver);
	}

	@Override
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
