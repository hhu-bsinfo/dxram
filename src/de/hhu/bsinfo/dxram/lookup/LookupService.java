
package de.hhu.bsinfo.dxram.lookup;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.log.LogService;
import de.hhu.bsinfo.dxram.logger.LoggerComponent;
import de.hhu.bsinfo.dxram.lookup.messages.*;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.net.NetworkErrorCodes;
import de.hhu.bsinfo.dxram.net.messages.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.util.NodeRole;
import de.hhu.bsinfo.ethnet.AbstractMessage;
import de.hhu.bsinfo.ethnet.NetworkHandler.MessageReceiver;

/**
 * Look up service providing look ups for e.g. use in TCMDs
 * @author Mike Birkhoff
 */

public class LookupService extends AbstractDXRAMService implements MessageReceiver {

	private AbstractBootComponent m_boot;
	private BackupComponent m_backup;
	private LoggerComponent m_logger;
	private NetworkComponent m_network;

	private LookupComponent m_lookup;

	/**
	 * Constructor
	 */
	public LookupService() {
		super("lookup");
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {

		m_boot = getComponent(AbstractBootComponent.class);
		m_backup = getComponent(BackupComponent.class);
		m_logger = getComponent(LoggerComponent.class);
		m_network = getComponent(NetworkComponent.class);
		m_lookup = getComponent(LookupComponent.class);

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
	 * @param p_message
	 *            the LookupTreeRequest
	 */
	private void incomingRequestLookupTreeOnServerMessage(final GetLookupTreeRequest p_message) {
		LookupTree tree = m_lookup.superPeerGetLookUpTree(p_message.getTreeNodeID());

		final NetworkErrorCodes err =
				m_network.sendMessage(new GetLookupTreeResponse(p_message, tree));

		if (err != NetworkErrorCodes.SUCCESS) {
			// #if LOGGER >= ERROR
			m_logger.error(LogService.class, "Could not acknowledge initilization of backup range: " + err);
			// #endif /* LOGGER >= ERROR */
		}

	}

	@Override
	public void onIncomingMessage(final AbstractMessage p_message) {

		if (p_message != null) {
			if (p_message.getType() == DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE) {
				switch (p_message.getSubtype()) {
					case LookupMessages.SUBTYPE_GET_LOOKUP_TREE_REQUEST:
						incomingRequestLookupTreeOnServerMessage((GetLookupTreeRequest) p_message);
						break;
					default:
						break;
				}
			}
		}

	}

	/**
	 * Returns all known superpeers
	 * @return array with all superpeers
	 */
	public ArrayList<Short> getAllSuperpeers() {
		return m_lookup.getAllSuperpeers();
	}

	/**
	 * Returns the responsible superpeer for given peer
	 * @param p_nid
	 *            node id to get responsible super peer from
	 * @return node ID of superpeer
	 */
	public short getResponsibleSuperpeer(final short p_nid) {
		return m_lookup.getResponsibleSuperpeer(p_nid);
	}

	/**
	 * sends a message to a superpeer to get a lookuptree from
	 * @param p_superPeerNid
	 *            superpeer where the lookuptree to get from
	 * @param p_nodeId
	 *            node id which lookuptree to get
	 * @return requested lookup Tree
	 */
	public LookupTree getLookupTreeFromSuperpeer(final short p_superPeerNid, final short p_nodeId) {

		LookupTree retTree;

		GetLookupTreeRequest lookupTreeRequest;
		GetLookupTreeResponse lookupTreeResponse;

		lookupTreeRequest = new GetLookupTreeRequest(p_superPeerNid, p_nodeId);

		if (m_network.sendSync(lookupTreeRequest) != NetworkErrorCodes.SUCCESS) {
			/* TODO err handling */
		}

		lookupTreeResponse = lookupTreeRequest.getResponse(GetLookupTreeResponse.class);
		retTree = lookupTreeResponse.getCIDTree();

		return retTree;
	}

	/**
	 * Sends a request to given superpeer to get a metadata summary
	 * @param p_nodeID
	 *            superpeer to get summary from
	 * @return the metadata summary
	 */
	public String getMetadataSummary(final short p_nodeID) {
		String ret;
		GetMetadataSummaryRequest request;
		GetMetadataSummaryResponse response;

		request = new GetMetadataSummaryRequest(p_nodeID);
		if (m_network.sendSync(request) != NetworkErrorCodes.SUCCESS) {
			return "Error!";
		}
		response = request.getResponse(GetMetadataSummaryResponse.class);
		ret = response.getMetadataSummary();

		return ret;
	}

	/**
	 * Register network messages we use in here.
	 */
	private void registerNetworkMessages() {
		m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_GET_LOOKUP_TREE_RESPONSE,
				GetLookupTreeResponse.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_GET_LOOKUP_TREE_REQUEST,
				GetLookupTreeRequest.class);

		m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_REQUEST,
				GetMetadataSummaryRequest.class);
		m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE,
				LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_RESPONSE,
				GetMetadataSummaryResponse.class);

	}

	/**
	 * Register network messages we want to listen to in here.
	 */
	private void registerNetworkMessageListener() {

		m_network.register(GetLookupTreeResponse.class, this);
		m_network.register(GetLookupTreeRequest.class, this);
	}

}
