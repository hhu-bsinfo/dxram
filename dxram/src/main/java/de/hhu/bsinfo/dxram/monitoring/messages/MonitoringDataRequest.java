package de.hhu.bsinfo.dxram.monitoring.messages;

import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

/**
 * Monitoring Request message (only used by terminal)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.07.2018
 */
public class MonitoringDataRequest extends Request {

    /**
     * Creates an instance of MonitoringRequest.
     * This constructor is used when receiving this message.
     */
    public MonitoringDataRequest() {
        super();
    }

    /**
     * Creates an instance of MonitoringRequest.
     * This constructor is used when sending this message.
     *
     * @param p_destination
     *     The destination nid.
     */
    public MonitoringDataRequest(final short p_destination) {
        super(p_destination, DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA_REQUEST);
    }

}
