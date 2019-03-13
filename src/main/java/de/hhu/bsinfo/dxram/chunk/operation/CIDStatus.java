package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxmem.data.ChunkIDRanges;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.GetLocalChunkIDRangesRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetLocalChunkIDRangesResponse;
import de.hhu.bsinfo.dxram.chunk.messages.GetMigratedChunkIDRangesRequest;
import de.hhu.bsinfo.dxram.chunk.messages.GetMigratedChunkIDRangesResponse;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxram.util.NodeRole;

/**
 * Get information about chunks stored
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class CIDStatus extends Operation implements MessageReceiver {
    public CIDStatus(final Class<? extends Service> p_parentService,
            final BootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
                ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST,
                GetLocalChunkIDRangesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
                ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_RESPONSE,
                GetLocalChunkIDRangesResponse.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
                ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST,
                GetMigratedChunkIDRangesRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
                ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_RESPONSE,
                GetMigratedChunkIDRangesResponse.class);

        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
                ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST, this);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE,
                ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST, this);
    }

    /**
     * Get all chunk ID ranges of all migrated chunks stored on this node.
     *
     * @return ChunkIDRanges of all migrated chunks.
     */
    public ChunkIDRanges getAllMigratedChunkIDRanges() {
        return m_chunk.getMemory().cidStatus().getAllMigratedChunkIDRanges();
    }

    /**
     * Get all chunk ID ranges of all locally stored chunks.
     *
     * @return Local ChunkIDRanges.
     */
    public ChunkIDRanges getAllLocalChunkIDRanges() {
        return m_chunk.getMemory().cidStatus().getCIDRangesOfLocalChunks();
    }

    /**
     * Get all chunk ID ranges of all stored chunks from a specific node.
     * This does not include migrated chunks.
     *
     * @param p_nodeID
     *         NodeID of the node to get the ranges from.
     * @return Local ChunkIDRanges
     */
    public ChunkIDRanges getAllLocalChunkIDRanges(final short p_nodeID) {
        ChunkIDRanges list;

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_nodeID);

        if (role == null) {
            m_logger.error("Remote node 0x%X does not exist for get local chunk id ranges", p_nodeID);
            return null;
        }

        if (p_nodeID == m_boot.getNodeId()) {
            list = getAllLocalChunkIDRanges();
        } else {
            GetLocalChunkIDRangesRequest request = new GetLocalChunkIDRangesRequest(p_nodeID);

            // important: the remote operation involves traversing the whole CIDTable which takes a while...
            // If the node fails during that process, the superpeer notifies all peers about that
            // and the NetworkService takes care of handling running requests that can't succeed anymore
            request.setIgnoreTimeout(true);

            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                m_logger.error("Sending request to get chunk id ranges of node 0x%X failed: %s", p_nodeID, e);
                return null;
            }

            GetLocalChunkIDRangesResponse response = (GetLocalChunkIDRangesResponse) request.getResponse();
            list = response.getChunkIDRanges();
        }

        return list;
    }

    /**
     * Get all migrated chunk IDs of all stored chunks from a specific node.
     *
     * @param p_nodeID
     *         NodeID of the node to get the IDs from.
     * @return Ranges of migrated chunk IDs
     */
    public ChunkIDRanges getAllMigratedChunkIDRanges(final short p_nodeID) {
        ChunkIDRanges list;

        // check if remote node is a peer
        NodeRole role = m_boot.getNodeRole(p_nodeID);

        if (role == null) {
            m_logger.error("Remote node 0x%X does not exist for get migrated chunk id ranges", p_nodeID);
            return null;
        }

        if (p_nodeID == m_boot.getNodeId()) {
            list = getAllMigratedChunkIDRanges();
        } else {
            GetMigratedChunkIDRangesRequest request = new GetMigratedChunkIDRangesRequest(p_nodeID);

            try {
                m_network.sendSync(request);
            } catch (final NetworkException e) {
                m_logger.error("Sending request to get chunk id ranges of node 0x%X failed: %s", p_nodeID, e);
                return null;
            }

            GetMigratedChunkIDRangesResponse response = (GetMigratedChunkIDRangesResponse) request.getResponse();
            list = response.getChunkIDRanges();
        }

        return list;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE) {
            switch (p_message.getSubtype()) {
                case ChunkMessages.SUBTYPE_GET_LOCAL_CHUNKID_RANGES_REQUEST:
                    incomingGetLocalChunkIDRangesRequest((GetLocalChunkIDRangesRequest) p_message);
                    break;
                case ChunkMessages.SUBTYPE_GET_MIGRATED_CHUNKID_RANGES_REQUEST:
                    incomingGetMigratedChunkIDRangesRequest((GetMigratedChunkIDRangesRequest) p_message);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * Handle incoming get local chunk id ranges requests.
     *
     * @param p_request
     *         Request to handle
     */
    private void incomingGetLocalChunkIDRangesRequest(final GetLocalChunkIDRangesRequest p_request) {
        GetLocalChunkIDRangesResponse response = new GetLocalChunkIDRangesResponse(p_request,
                m_chunk.getMemory().cidStatus().getCIDRangesOfLocalChunks());

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            m_logger.error("Responding to local chunk id ranges request %s failed: %s", p_request, e);
        }
    }

    /**
     * Handle incoming get migrated local chunk id ranges requests.
     *
     * @param p_request
     *         Request to handle
     */
    private void incomingGetMigratedChunkIDRangesRequest(final GetMigratedChunkIDRangesRequest p_request) {
        GetMigratedChunkIDRangesResponse response = new GetMigratedChunkIDRangesResponse(p_request,
                m_chunk.getMemory().cidStatus().getAllMigratedChunkIDRanges());

        try {
            m_network.sendMessage(response);
        } catch (final NetworkException e) {
            m_logger.error("Responding to migrated chunk id ranges request %s failed: %s", p_request, e);
        }
    }
}
