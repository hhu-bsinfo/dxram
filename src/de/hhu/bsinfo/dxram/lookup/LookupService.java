
package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMEngineConfigurationValues;
import de.hhu.bsinfo.dxram.lock.AbstractLockComponent;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.messages.LookupTreeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.RequestLookupTreeFromSuperPeer;
import de.hhu.bsinfo.dxram.lookup.messages.RequestResponsibleSuperPeer;
import de.hhu.bsinfo.dxram.lookup.messages.RequestSendLookupTreeMessage;
import de.hhu.bsinfo.dxram.lookup.messages.ResponseResponsibleSuperPeer;
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

/**
 * Look up service providing look ups for e.g. use in TCMDs
 * @author Mike Birkhoff
 */

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

	private void incomingRequestLookupTreeOnServerMessage(RequestLookupTreeFromSuperPeer p_message) {

		System.out.println("incomingRequestSendLookupTreeFromServerMessage "
				+ "from " + p_message.getTreeNodeID() + "to" + p_message.getDestination());

		LookupTree tree = m_lookup.superPeerGetLookUpTree(p_message.getTreeNodeID());

		final NetworkErrorCodes err =
				m_network.sendMessage(new LookupTreeResponse(p_message, tree));
		if (err != NetworkErrorCodes.SUCCESS) {
			m_logger.error(LogService.class, "Could not acknowledge initilization of backup range: " + err);
		}

	}

	private void incomingSendLookupTreeMessage(LookupTreeResponse p_message) {

		System.out.println("incoming Send LookupTree Message:");
		if (p_message.getCIDTree() != null)
			System.out.println(p_message.getCIDTree().toString());
		else
			System.out.println("Lookup Tree Tree is null");
	}

	private void incomingRequestResponsibleSuperPeer(RequestResponsibleSuperPeer p_message) {

		short superPeerID = m_lookup.getMyResponsibleSuperPeer();

		if (m_network
				.sendMessage(new ResponseResponsibleSuperPeer(p_message, superPeerID)) != NetworkErrorCodes.SUCCESS) {
			m_logger.error(LogService.class, "could not send requested super peer response ");
		}

	}

	@Override
	public void onIncomingMessage(AbstractMessage p_message) {

		System.out.println("incoming message");
		if (p_message != null) {
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_SEND_LOOK_UP_TREE:
						incomingSendLookupTreeMessage((LookupTreeResponse) p_message);
						break;
					case LookupMessages.SUBTYPE_REQUEST_LOOK_UP_TREE_FROM_SERVER:
						incomingRequestLookupTreeOnServerMessage((RequestLookupTreeFromSuperPeer) p_message);
						break;
					default:
						break;
					case LookupMessages.SUBTYPE_REQUEST_RESPONSIBLE_SUPERPEER:
						incomingRequestResponsibleSuperPeer((RequestResponsibleSuperPeer) p_message);
						break;
					case LookupMessages.SUBTYPE_RESPONSE_RESPONSIBLE_SUPERPEER:
						break;
				}
			}
		}

	}

	public short getResponsibleSuperPeer(short p_nid) {

		// // first send a message to Node which Tree is requested and then send a request to the responsible super peer
		// final NetworkErrorCodes err = m_network.sendMessage(new RequestSendLookupTreeMessage(p_nid));
		// if (err != NetworkErrorCodes.SUCCESS) {
		// m_logger.error(LogService.class, "Could not acknowledge initilization of backup range: " + err);
		// }

		short responsibleSuperPeer = -1;
		RequestResponsibleSuperPeer superPeerRequest;
		ResponseResponsibleSuperPeer superPeerResponse;

		while (-1 == responsibleSuperPeer) {

			superPeerRequest = new RequestResponsibleSuperPeer(p_nid);
			if (m_network.sendSync(superPeerRequest) != NetworkErrorCodes.SUCCESS) {
				continue;
			}

			superPeerResponse = superPeerRequest.getResponse(ResponseResponsibleSuperPeer.class);
			responsibleSuperPeer = superPeerResponse.getResponsibleSuperPeer();

		}

		//
		// while (-1 != contactSuperpeer) {
		// // #if LOGGER == TRACE
		// m_logger.trace(getClass(), "Contacting " + NodeID.toHexString(contactSuperpeer)
		// + " to join the ring, I am " + NodeID.toHexString(m_nodeID));
		// // #endif /* LOGGER == TRACE */
		//
		// joinRequest = new JoinRequest(contactSuperpeer, m_nodeID, IS_SUPERPEER);
		// if (m_network.sendSync(joinRequest) != NetworkErrorCodes.SUCCESS) {
		// // Contact superpeer is not available, get a new contact superpeer
		// contactSuperpeer = m_boot.getNodeIDBootstrap();
		// continue;
		// }
		//
		// joinResponse = joinRequest.getResponse(JoinResponse.class);
		// contactSuperpeer = joinResponse.getNewContactSuperpeer();
		// }
		//

		return responsibleSuperPeer;
	}

	public LookupTree getLookupTreeFromSuperPeer(short p_superPeerNid, short p_nodeId) {

		LookupTree retTree = null;

		RequestLookupTreeFromSuperPeer lookupTreeRequest;
		LookupTreeResponse lookupTreeResponse;

		while (null == retTree) {

			lookupTreeRequest = new RequestLookupTreeFromSuperPeer(p_superPeerNid, p_nodeId);
			if (m_network.sendSync(lookupTreeRequest) != NetworkErrorCodes.SUCCESS) {
				continue;
			}

			lookupTreeResponse = lookupTreeRequest.getResponse(LookupTreeResponse.class);
			retTree = lookupTreeResponse.getCIDTree();

		}

		return retTree;
	}

	// -----------------------------------------------------------------------------------

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_SEND_LOOK_UP_TREE,
				LookupTreeResponse.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REQUEST_LOOK_UP_TREE_FROM_SERVER,
				RequestLookupTreeFromSuperPeer.class);

		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_REQUEST_RESPONSIBLE_SUPERPEER,
				RequestResponsibleSuperPeer.class);
		m_network.registerMessageType(LookupMessages.TYPE, LookupMessages.SUBTYPE_RESPONSE_RESPONSIBLE_SUPERPEER,
				ResponseResponsibleSuperPeer.class);

	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {

		m_network.register(RequestSendLookupTreeMessage.class, this);
		m_network.register(LookupTreeResponse.class, this);
		m_network.register(RequestLookupTreeFromSuperPeer.class, this);
		m_network.register(RequestResponsibleSuperPeer.class, this);
		m_network.register(ResponseResponsibleSuperPeer.class, this);
	}

}
