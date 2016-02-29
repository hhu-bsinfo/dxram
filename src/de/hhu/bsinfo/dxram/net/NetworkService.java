package de.hhu.bsinfo.dxram.net;

import de.hhu.bsinfo.dxram.engine.DXRAMService;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.AbstractRequest;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

public class NetworkService extends DXRAMService {

	private NetworkComponent m_network = null;
	
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		m_network.registerMessageType(p_type, p_subtype, p_class);
	}
	
	public void registerReceiver(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver) {
		m_network.register(p_message, p_receiver);
	}

	public void unregisterReceiver(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver) {
		m_network.unregister(p_message, p_receiver);
	}
	
	public NetworkErrorCodes sendMessage(final AbstractMessage p_message) {
		return m_network.sendMessage(p_message);
	}
	
	public NetworkErrorCodes sendSync(final AbstractRequest p_request) {
		return m_network.sendSync(p_request);
	}
	
	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {

	}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {
		m_network = getComponent(NetworkComponent.class);
		
		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_network = null;
		
		return true;
	}

}
