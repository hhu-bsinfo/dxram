/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science,
 * Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public
 * License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxram.monitoring.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

/**
 * Monitoring Response message (only used by terminal)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
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
