package de.hhu.bsinfo.dxram.monitoring.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.monitoring.MonitoringSysInfoDataStructure;

/**
 * Monitoring message with system information
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class MonitoringSysInfoMessage extends Message {

    private MonitoringSysInfoDataStructure monitoringSysInfoDataStructure;

    public MonitoringSysInfoMessage() {
        monitoringSysInfoDataStructure = new MonitoringSysInfoDataStructure();
    }

    public MonitoringSysInfoMessage(short p_destination, MonitoringSysInfoDataStructure p_dataStructure) {
        super(p_destination, DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO);

        monitoringSysInfoDataStructure = p_dataStructure;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.exportObject(monitoringSysInfoDataStructure);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        p_importer.importObject(monitoringSysInfoDataStructure);
    }

    @Override
    protected int getPayloadLength() {
        return monitoringSysInfoDataStructure.sizeofObject();
    }

    public MonitoringSysInfoDataStructure getMonitoringSysInfoDataStructure() {
        return monitoringSysInfoDataStructure;
    }
}
