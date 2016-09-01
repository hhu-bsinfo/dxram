
package de.hhu.bsinfo.dxram.lookup;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
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
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.term.TerminalComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.menet.AbstractMessage;
import de.hhu.bsinfo.menet.NetworkHandler.MessageReceiver;
import de.hhu.bsinfo.menet.NodeID;

/**
 * Look up service providing look ups for e.g. use in TCMDs
 *
 * @author Mike Birkhoff
 */

public class LookupService extends AbstractDXRAMService implements MessageReceiver {

	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private LoggerComponent m_logger;
	private NetworkComponent m_network;
	private TerminalComponent m_terminal;

	private LookupComponent m_lookup;

	/**
	 * Constructor
	 */
	public LookupService() {
		super();
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {
	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		m_boot = getComponent(AbstractBootComponent.class);
		m_backup = getComponent(BackupComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);

		//
		m_lookup = getComponent(LookupComponent.class);
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

		m_network = null;
		m_lookup = null;

		return true;
	}

	/**
	 * Sends a Response to a LookupTree Request
	 *
	 * @param p_message the LookupTreeRequest
	 */
	private void incomingRequestLookupTreeOnServerMessage(final RequestLookupTreeFromSuperPeer p_message) {
		LookupTree tree = m_lookup.superPeerGetLookUpTree(p_message.getTreeNodeID());

		final NetworkErrorCodes err =
				m_network.sendMessage(new LookupTreeResponse(p_message, tree));

		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(LogService.class, "Could not acknowledge initilization of backup range: " + err);
			// #endif
		}

	}

	/**
	 * handles the request for the responsible Superpeer of this node
	 *
	 * @param p_message the Request for the ResponsibleSuperPeer
	 */
	private void incomingRequestResponsibleSuperPeer(final RequestResponsibleSuperPeer p_message) {
		short superPeerID = m_lookup.getMyResponsibleSuperPeer();

		if (m_network
				.sendMessage(new ResponseResponsibleSuperPeer(p_message, superPeerID)) != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(LogService.class, "could not send requested super peer response ");
			// #endif
		}

	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {

		if (p_message != null) {
			if (p_message.getType() == LookupMessages.TYPE) {
				switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_REQUEST_LOOK_UP_TREE_FROM_SERVER:
						incomingRequestLookupTreeOnServerMessage((RequestLookupTreeFromSuperPeer) p_message);
						break;
					case LookupMessages.SUBTYPE_REQUEST_RESPONSIBLE_SUPERPEER:
						incomingRequestResponsibleSuperPeer((RequestResponsibleSuperPeer) p_message);
						break;
					case LookupMessages.SUBTYPE_RESPONSE_RESPONSIBLE_SUPERPEER:
						break;
					default:
						break;
				}
			}
		}

	}

	/**
	 * Sends a method to a node id to request the responsible peer
	 *
	 * @param p_nid node id to get responsible super peer from
	 * @return node ID of superpeer
	 */
	public short getResponsibleSuperPeer(final short p_nid) {

		short responsibleSuperPeer = NodeID.INVALID_ID;
		RequestResponsibleSuperPeer superPeerRequest;
		ResponseResponsibleSuperPeer superPeerResponse;

		superPeerRequest = new RequestResponsibleSuperPeer(p_nid);
		if (m_network.sendSync(superPeerRequest) != NetworkErrorCodes.SUCCESS) {
			/* TODO err handling */
		}

		superPeerResponse = superPeerRequest.getResponse(ResponseResponsibleSuperPeer.class);
		responsibleSuperPeer = superPeerResponse.getResponsibleSuperPeer();

		return responsibleSuperPeer;
	}

	/**
	 * sends a message to a superpeer to get a lookuptree from
	 *
	 * @param p_superPeerNid superpeer where the lookuptree to get from
	 * @param p_nodeId       node id which lookuptree to get
	 * @return requested lookup Tree
	 */
	public LookupTree getLookupTreeFromSuperPeer(final short p_superPeerNid, final short p_nodeId) {

		LookupTree retTree;

		RequestLookupTreeFromSuperPeer lookupTreeRequest;
		LookupTreeResponse lookupTreeResponse;

		lookupTreeRequest = new RequestLookupTreeFromSuperPeer(p_superPeerNid, p_nodeId);

		if (m_network.sendSync(lookupTreeRequest) != NetworkErrorCodes.SUCCESS) {
			/* TODO err handling */
		}

		lookupTreeResponse = lookupTreeRequest.getResponse(LookupTreeResponse.class);
		retTree = lookupTreeResponse.getCIDTree();

		return retTree;
	}

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
