/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.lookup;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.boot.AbstractBootComponent;
import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;
import de.hhu.bsinfo.dxram.engine.DXRAMComponentAccessor;
import de.hhu.bsinfo.dxram.engine.DXRAMContext;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupTreeRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetLookupTreeResponse;
import de.hhu.bsinfo.dxram.lookup.messages.GetMetadataSummaryRequest;
import de.hhu.bsinfo.dxram.lookup.messages.GetMetadataSummaryResponse;
import de.hhu.bsinfo.dxram.lookup.messages.LookupMessages;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.LookupTree;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;

/**
 * Look up service providing look ups for e.g. use in TCMDs
 *
 * @author Mike Birkhoff, michael.birkhoff@hhu.de, 15.07.2016
 */
public class LookupService extends AbstractDXRAMService<LookupServiceConfig> implements MessageReceiver {
    // component dependencies
    private AbstractBootComponent m_boot;
    private NetworkComponent m_network;
    private LookupComponent m_lookup;

    /**
     * Constructor
     */
    public LookupService() {
        super("lookup", LookupServiceConfig.class);
    }

    /**
     * Returns all known superpeers
     *
     * @return array with all superpeers
     */
    public ArrayList<Short> getAllSuperpeers() {
        return m_lookup.getAllSuperpeers();
    }

    @Override
    public void onIncomingMessage(final Message p_message) {

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
     * Get the corresponding primary peer (the peer storing the Chunk in RAM) for the given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the primary peer
     */
    public short getPrimaryPeer(final long p_chunkID) {
        return m_lookup.getPrimaryPeer(p_chunkID);
    }

    /**
     * Get the corresponding LookupRange for the given ChunkID
     *
     * @param p_chunkID
     *         the ChunkID
     * @return the current location and the range borders
     */
    public LookupRange getLookupRange(final long p_chunkID) {
        return m_lookup.getLookupRange(p_chunkID);
    }

    /**
     * Returns the responsible superpeer for given peer
     *
     * @param p_nid
     *         node id to get responsible super peer from
     * @return node ID of superpeer
     */
    public short getResponsibleSuperpeer(final short p_nid) {
        return m_lookup.getResponsibleSuperpeer(p_nid);
    }

    /**
     * sends a message to a superpeer to get a lookuptree from
     *
     * @param p_superPeerNid
     *         superpeer where the lookuptree to get from
     * @param p_nodeId
     *         node id which lookuptree to get
     * @return requested lookup Tree
     */
    public LookupTree getLookupTreeFromSuperpeer(final short p_superPeerNid, final short p_nodeId) {

        LookupTree retTree;

        GetLookupTreeRequest lookupTreeRequest;
        GetLookupTreeResponse lookupTreeResponse;

        lookupTreeRequest = new GetLookupTreeRequest(p_superPeerNid, p_nodeId);

        try {
            m_network.sendSync(lookupTreeRequest);
        } catch (final NetworkException ignored) {
            return null;
        }

        lookupTreeResponse = lookupTreeRequest.getResponse(GetLookupTreeResponse.class);
        retTree = lookupTreeResponse.getCIDTree();

        return retTree;
    }

    /**
     * Sends a request to given superpeer to get a metadata summary
     *
     * @param p_nodeID
     *         superpeer to get summary from
     * @return the metadata summary
     */
    public String getMetadataSummary(final short p_nodeID) {
        String ret;
        GetMetadataSummaryRequest request;
        GetMetadataSummaryResponse response;

        request = new GetMetadataSummaryRequest(p_nodeID);

        try {
            m_network.sendSync(request);
        } catch (final NetworkException ignored) {
            return "Error!";
        }

        response = request.getResponse(GetMetadataSummaryResponse.class);
        ret = response.getMetadataSummary();

        return ret;
    }

    @Override
    protected boolean supportsSuperpeer() {
        return true;
    }

    @Override
    protected boolean supportsPeer() {
        return true;
    }

    @Override
    protected void resolveComponentDependencies(final DXRAMComponentAccessor p_componentAccessor) {
        m_boot = p_componentAccessor.getComponent(AbstractBootComponent.class);
        m_network = p_componentAccessor.getComponent(NetworkComponent.class);
        m_lookup = p_componentAccessor.getComponent(LookupComponent.class);
    }

    @Override
    protected boolean startService(final DXRAMContext.Config p_config) {
        registerNetworkMessages();
        registerNetworkMessageListener();

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
     * @param p_message
     *         the LookupTreeRequest
     */
    private void incomingRequestLookupTreeOnServerMessage(final GetLookupTreeRequest p_message) {
        LookupTree tree = m_lookup.superPeerGetLookUpTree(p_message.getTreeNodeID());

        try {
            m_network.sendMessage(new GetLookupTreeResponse(p_message, tree));
        } catch (final NetworkException e) {
            // #if LOGGER >= ERROR
            LOGGER.error("Could not acknowledge initilization of backup range: %s", e);
            // #endif /* LOGGER >= ERROR */
        }
    }

    /**
     * Register network messages we use in here.
     */
    private void registerNetworkMessages() {
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_REQUEST, GetLookupTreeRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_RESPONSE, GetLookupTreeResponse.class);

        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_REQUEST,
                GetMetadataSummaryRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_METADATA_SUMMARY_RESPONSE,
                GetMetadataSummaryResponse.class);

    }

    /**
     * Register network messages we want to listen to in here.
     */
    private void registerNetworkMessageListener() {
        m_network.register(DXRAMMessageTypes.LOOKUP_MESSAGES_TYPE, LookupMessages.SUBTYPE_GET_LOOKUP_TREE_REQUEST, this);
    }
}
