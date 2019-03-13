package de.hhu.bsinfo.dxram.chunk.operation;

import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.backup.BackupComponent;
import de.hhu.bsinfo.dxram.boot.BootComponent;
import de.hhu.bsinfo.dxram.chunk.ChunkComponent;
import de.hhu.bsinfo.dxram.chunk.data.ChunkServiceStatus;
import de.hhu.bsinfo.dxram.chunk.messages.ChunkMessages;
import de.hhu.bsinfo.dxram.chunk.messages.StatusRequest;
import de.hhu.bsinfo.dxram.chunk.messages.StatusResponse;
import de.hhu.bsinfo.dxram.engine.Service;
import de.hhu.bsinfo.dxram.lookup.LookupComponent;
import de.hhu.bsinfo.dxram.nameservice.NameserviceComponent;
import de.hhu.bsinfo.dxram.net.NetworkComponent;
import de.hhu.bsinfo.dxutils.NodeID;

/**
 * Get the status of the chunk service (locally and remotely)
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 31.08.2018
 */
public class Status extends Operation implements MessageReceiver {
    /**
     * Constructor
     *
     * @param p_parentService
     *         Instance of parent service this operation belongs to
     * @param p_boot
     *         Instance of BootComponent
     * @param p_backup
     *         Instance of BackupComponent
     * @param p_chunk
     *         Instance of ChunkComponent
     * @param p_network
     *         Instance of NetworkComponent
     * @param p_lookup
     *         Instance of LookupComponent
     * @param p_nameservice
     *         Instance of NameserviceComponent
     */
    public Status(final Class<? extends Service> p_parentService,
            final BootComponent p_boot, final BackupComponent p_backup, final ChunkComponent p_chunk,
            final NetworkComponent p_network, final LookupComponent p_lookup,
            final NameserviceComponent p_nameservice) {
        super(p_parentService, p_boot, p_backup, p_chunk, p_network, p_lookup, p_nameservice);

        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_REQUEST,
                StatusRequest.class);
        m_network.registerMessageType(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_RESPONSE,
                StatusResponse.class);
        m_network.register(DXRAMMessageTypes.CHUNK_MESSAGES_TYPE, ChunkMessages.SUBTYPE_STATUS_REQUEST, this);
    }

    /**
     * Get the status of the chunk service
     *
     * @return Instance with chunk service status
     */
    public ChunkServiceStatus getStatus() {
        return new ChunkServiceStatus(m_chunk.getMemory().stats().getHeapStatus(),
                m_chunk.getMemory().stats().getCIDTableStatus(), m_chunk.getMemory().stats().getLIDStoreStatus());
    }

    /**
     * Get the status of the chunk service
     *
     * @param p_nodeID
     *         Remote node to get status from
     * @return Instance with chunk service status or null on error
     */
    public ChunkServiceStatus getStatus(final short p_nodeID) {
        ChunkServiceStatus status = null;

        if (p_nodeID == NodeID.INVALID_ID) {
            m_logger.error("Invalid node id on get status");
            return null;
        }

        // own status?
        if (p_nodeID == m_boot.getNodeId()) {
            status = getStatus();
        } else {
            // grab from remote
            StatusRequest request = new StatusRequest(p_nodeID);

            try {
                m_network.sendSync(request);

                StatusResponse response = request.getResponse(StatusResponse.class);
                status = response.getStatus();
            } catch (final NetworkException e) {
                m_logger.error("Sending get status request to peer %s failed: %s", NodeID.toHexString(p_nodeID), e);
            }
        }

        return status;
    }

    @Override
    public void onIncomingMessage(final Message p_message) {
        if (p_message.getType() == DXRAMMessageTypes.CHUNK_MESSAGES_TYPE &&
                p_message.getSubtype() == ChunkMessages.SUBTYPE_STATUS_REQUEST) {
            StatusRequest request = (StatusRequest) p_message;

            StatusResponse response = new StatusResponse(request, getStatus());

            try {
                m_network.sendMessage(response);
            } catch (final NetworkException e) {
                m_logger.error("Sending status respond to request %s failed: %s", request, e);
            }
        }
    }
}
