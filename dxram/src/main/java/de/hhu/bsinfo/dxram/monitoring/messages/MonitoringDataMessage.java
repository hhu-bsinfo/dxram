/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.monitoring.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.monitoring.MonitoringDataStructure;

/**
 * Message with monitoring data.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringDataMessage extends Message {
    /**
     * cpuUsage
     * loads (1, 5, 15)
     * memoryUsage
     * receiveThroughput
     * transmitThroughput
     * receiveError
     * transmitError
     * readPercentage
     * writePercentage
     * jvmHeapUsage
     * jvmEdenUsage
     * jvmSurvivorUsage
     * jvmOldUsage
     */
    private float[] m_data; // data gets filled in component handler
    /**
     * jvm Thread stats -> #daemon, #nondaemon, #threadcnt, #peak
     */
    private long[] m_data2;
    private long m_timestamp;

    /**
     * Constructor
     */
    public MonitoringDataMessage() {
        super();

        m_data = new float[15];
        for (int i = 0; i < 15; i++) {
            m_data[i] = 0;
        }

        m_data2 = new long[4];
        for (int i = 0; i < 4; i++) {
            m_data[i] = 4;
        }

    }

    /**
     * Constructor
     *
     * @param p_destination destination nid
     * @param p_data        data to send
     */
    public MonitoringDataMessage(final short p_destination, MonitoringDataStructure p_data) {
        super(p_destination, DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_DATA);

        m_data = new float[15];
        m_data[0] = p_data.getCpuUsage();
        float[] tmp = p_data.getCpuLoads();
        m_data[1] = tmp[0];
        m_data[2] = tmp[1];
        m_data[3] = tmp[2];
        m_data[4] = p_data.getMemoryUsage();
        tmp = p_data.getNetworkStats();
        m_data[5] = tmp[0];
        m_data[6] = tmp[1];
        m_data[7] = tmp[2];
        m_data[8] = tmp[3];
        tmp = p_data.getDiskStats();
        m_data[9] = tmp[0];
        m_data[10] = tmp[1];
        tmp = p_data.getJvmMemStats();
        m_data[11] = tmp[0];
        m_data[12] = tmp[1];
        m_data[13] = tmp[2];
        m_data[14] = tmp[3];

        m_data2 = new long[4];
        long[] tmp2 = p_data.getJvmThreadStats();
        for (int i = 0; i < 4; i++) {
            m_data2[i] = tmp2[i];
        }

        m_timestamp = p_data.getTimestamp();
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(m_timestamp);
        p_exporter.writeInt(m_data.length);
        for (int i = 0; i < m_data.length; i++) {
            p_exporter.writeFloat(m_data[i]);
        }
        p_exporter.writeInt(m_data2.length);
        for (int i = 0; i < m_data2.length; i++) {
            p_exporter.writeLong(m_data2[i]);
        }
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_timestamp = p_importer.readLong(m_timestamp);
        int length = p_importer.readInt(15);
        m_data = new float[length];
        for (int i = 0; i < length; i++) {
            m_data[i] = p_importer.readFloat(m_data[i]);
        }
        length = p_importer.readInt(4);
        m_data2 = new long[length];
        for (int i = 0; i < length; i++) {
            m_data2[i] = p_importer.readLong(m_data2[i]);
        }
    }

    @Override
    protected int getPayloadLength() {
        return Long.BYTES + Integer.BYTES + Float.BYTES * m_data.length + Integer.BYTES + Long.BYTES * m_data2.length;
    }

    /**
     * Sets the timestamp.
     */
    public void setTimestamp(final long p_timestamp) {
        m_timestamp = p_timestamp;
    }

    /**
     * Returns timestamp of data
     */
    public long getTimestamp() {
        return m_timestamp;
    }

    /**
     * Take message data and create data structure out of it
     *
     * @return component data.
     */
    public MonitoringDataStructure getMonitorData() {
        return new MonitoringDataStructure(getSource(), m_data, m_data2, m_timestamp);
    }
}
