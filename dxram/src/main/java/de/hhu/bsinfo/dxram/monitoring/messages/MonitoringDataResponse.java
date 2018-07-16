package de.hhu.bsinfo.dxram.monitoring.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

/**
 * Monitoring Response message (only used by terminal)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class MonitoringDataResponse extends Response {
    private MonitoringDataStructure m_data;

    /**
     * Creates an instance of MonitoringRequest.
     * This constructor is used when receiving this message.
     */
    public MonitoringDataResponse() {
        super();
    }

    /**
     * Creates an instance of MonitoringResponse.
     * This constructor is used when sending this message.
     */
    public MonitoringDataResponse(final MonitoringDataRequest p_request, MonitoringDataStructure p_monitoringData) {
        super(p_request, MonitoringMessages.SUBTYPE_MONITORING_DATA_RESPONSE);
        m_data = p_monitoringData;
    }

    /**
     * Get monitoring data.
     *
     * @return Data
     */
    public MonitoringDataStructure getData() {
        return m_data;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(m_data);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        p_importer.importObject(m_data);
    }

    @Override
    protected int getPayloadLength() {
        return m_data.sizeofObject();
    }
}
