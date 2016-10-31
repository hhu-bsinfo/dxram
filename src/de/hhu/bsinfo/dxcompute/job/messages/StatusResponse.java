
package de.hhu.bsinfo.dxcompute.job.messages;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.dxcompute.job.JobService;
import de.hhu.bsinfo.dxram.data.MessagesDataStructureImExporter;
import de.hhu.bsinfo.ethnet.AbstractResponse;

/**
 * Response to the status request to get information about remote job systems.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class StatusResponse extends AbstractResponse {
    private JobService.Status m_status;

    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when receiving this message.
     */
    public StatusResponse() {
        super();
    }

    /**
     * Creates an instance of StatusResponse.
     * This constructor is used when sending this message.
     * @param p_request
     *            the corresponding StatusRequest
     * @param p_status
     *            the requested Status
     */
    public StatusResponse(final StatusRequest p_request, final JobService.Status p_status) {
        super(p_request, JobMessages.SUBTYPE_STATUS_RESPONSE);

        m_status = p_status;
    }

    /**
     * Get the job service status.
     * @return Job service status.
     */
    public final JobService.Status getStatus() {
        return m_status;
    }

    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter exporter = new MessagesDataStructureImExporter(p_buffer);
        exporter.exportObject(m_status);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        MessagesDataStructureImExporter importer = new MessagesDataStructureImExporter(p_buffer);
        importer.importObject(m_status);
    }

    @Override
    protected final int getPayloadLength() {
        return m_status.sizeofObject();
    }
}
