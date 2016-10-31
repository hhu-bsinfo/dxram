
package de.hhu.bsinfo.dxcompute.ms.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.ms.MasterSlaveComputeService.StatusMaster;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the request to get the status of a master compute node.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 22.04.2016
 */
public class GetMasterStatusResponse extends AbstractResponse {
    private StatusMaster m_statusMaster;

    /**
     * Creates an instance of GetMasterStatusResponse.
     * This constructor is used when receiving this message.
     */
    public GetMasterStatusResponse() {
        super();
    }

    /**
     * Creates an instance of GetMasterStatusResponse.
     * This constructor is used when sending this message.
     * @param p_request
     *            request to respond to.
     * @param p_statusMaster
     *            Status data of the master to send back
     */
    public GetMasterStatusResponse(final GetMasterStatusRequest p_request, final StatusMaster p_statusMaster) {
        super(p_request, MasterSlaveMessages.SUBTYPE_GET_MASTER_STATUS_RESPONSE);

        m_statusMaster = p_statusMaster;
    }

    /**
     * Current status of the master.
     * @return Status of the master.
     */
    public StatusMaster getStatusMaster() {
        return m_statusMaster;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);

        if (m_statusMaster != null) {
            exporter.exportObject(m_statusMaster);
        }
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);

        if (getStatusCode() == 0) {
            m_statusMaster = new StatusMaster();
            importer.importObject(m_statusMaster);
        }
    }

    @Override
    protected final int getPayloadLength() {
        if (m_statusMaster != null) {
            return m_statusMaster.sizeofObject();
        } else {
            return 0;
        }
    }
}
