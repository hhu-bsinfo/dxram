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
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Monitoring propose message - will be send by callback functions automatically "in the background"
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringProposeMessage extends Message {

    private String m_component;
    private double m_value;

    /**
     * Constructor
     */
    public MonitoringProposeMessage() {
    }

    /**
     * Constructor
     *
     * @param p_destination destination nid
     * @param p_component   component name
     */
    public MonitoringProposeMessage(final short p_destination, String p_component, double p_value) {
        super(p_destination, DXRAMMessageTypes.MONITORING_MESSAGES_TYPE, MonitoringMessages.SUBTYPE_MONITORING_PROPOSE);
        m_component = p_component;
        m_value = p_value;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_component);
        p_exporter.writeDouble(m_value);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_component = p_importer.readString(m_component);
        m_value = p_importer.readDouble(m_value);
    }

    @Override
    protected int getPayloadLength() {
        return ObjectSizeUtil.sizeofString(m_component) + Double.BYTES;
    }

    /**
     * Returns critical value.
     */
    public double getValue() {
        return m_value;
    }

    /**
     * Returns critical component name.
     */
    public String getComponent() {
        return m_component;
    }
}
