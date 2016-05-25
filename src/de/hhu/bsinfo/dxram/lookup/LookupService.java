
package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngineConfigurationValues;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.messages.RequestSendLookupTreeMessage;
import de.hhu.bsinfo.dxram.lookup.messages.SendLookupTreeMessage;
import de.hhu.bsinfo.dxram.lookup.overlay.LookupTree;
import de.hhu.bsinfo.dxram.lookup.tcmds.TcmdPrintLookUpTree;
import de.hhu.bsinfo.dxram.mem.MemoryManagerComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.stats.StatisticsComponent;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;

public class LookupService extends AbstractDXRAMService implements MessageReceiver {

	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private LoggerComponent m_logger;
	private MemoryManagerComponent m_memoryManager;
	private NetworkComponent m_network;
	private AbstractLockComponent m_lock;
	private StatisticsComponent m_statistics;
	private TerminalComponent m_terminal;

	private LookupComponent m_lookup;
	private short m_nodeID;

	private boolean m_performanceFlag;

	/**
	 * Constructor
	 */
	public LookupService() {
		super();
	}

	@Override
	protected void registerDefaultSettingsService(Settings p_settings) {}

	@Override
	protected boolean startService(de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			Settings p_settings) {

		m_performanceFlag = p_engineSettings.getValue(DXRAMEngineConfigurationValues.PERFORMANCE_FLAG);

		m_boot = getComponent(AbstractBootComponent.class);
		m_backup = getComponent(BackupComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_memoryManager = getComponent(MemoryManagerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);
		m_lock = getComponent(AbstractLockComponent.class);
		m_statistics = getComponent(StatisticsComponent.class);

		//
		m_lookup = getComponent(LookupComponent.class);
		m_nodeID = getComponent(AbstractBootComponent.class).getNodeID();
		m_terminal = getComponent(TerminalComponent.class);
		m_terminal.registerCommand(new TcmdPrintLookUpTree());

		registerNetworkMessages();
		registerNetworkMessageListener();

		if (m_boot.getNodeRole().equals(NodeRole.PEER)) {
			m_backup.registerPeer();
		}

		return true;
	}

	@Override
	protected boolean shutdownService() {

		m_memoryManager = null;
		m_network = null;
		m_lookup = null;

		return true;
	}

	private void incomingRequestSendLookupTreeMessage(RequestSendLookupTreeMessage p_message) {

		short dest = p_message.getSource();
		// To Do getlookup tree from tree component only if super peer!!
		// ArrayList<LookupTree> tree = new ArrayList<>(Arrays.asList(m_lookup.getSuperPeerLookUpTree(m_nodeID)));

		LookupTree tree = m_lookup.getSuperPeerLookUpTree(m_nodeID);

		final NetworkErrorCodes err = m_network.sendMessage(new SendLookupTreeMessage(dest, tree));
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(LogService.class, "Could not acknowledge initilization of backup range: " + err);
		}
	}

	private void incomingSendLookupTreeMessage(SendLookupTreeMessage p_message) {

		System.out.println("incoming Send LookupTree Message:");
		if (p_message.getCIDTree() != null)
			System.out.println(p_message.getCIDTree().toString());
		else
			System.out.println("Migration Tree is null");
	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {

		if (p_message != null) {
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_REQUEST_SEND_LOOK_UP_TREE:
						incomingRequestSendLookupTreeMessage((RequestSendLookupTreeMessage) p_message);
						break;
					case LookupMessages.SUBTYPE_SEND_LOOK_UP_TREE:
						incomingSendLookupTreeMessage((SendLookupTreeMessage) p_message);
						break;
					default:
						break;
				}
			}
		}

	}

	public void getLookupTree(short nid) {

		final NetworkErrorCodes err = m_network.sendMessage(new RequestSendLookupTreeMessage(nid, m_nodeID));
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(LogService.class, "Could not acknowledge initilization of backup range: " + err);
		}
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REQUEST_SEND_LOOK_UP_TREE,
				RequestSendLookupTreeMessage.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_LOOK_UP_TREE,
				SendLookupTreeMessage.class);

	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {

		m_network.register(RequestSendLookupTreeMessage.class, this);
		m_network.register(SendLookupTreeMessage.class, this);
	}

}
