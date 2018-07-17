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
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.monitoring.util.MonitoringSysDxramWrapper;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Monitoring message with system information
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringSysInfoMessage extends Message {
    private String[] m_sysInfos;
    private String[] m_dxramInfos;

    private boolean m_isPageCacheInUse;

    /**
     * Constructor
     */
    public MonitoringSysInfoMessage() {
        m_sysInfos = new String[5];
        m_dxramInfos = new String[5];
        m_isPageCacheInUse = true; // default values
    }

    /**
     * Constructor
     *
     * @param p_destination nid of destination node
     * @param p_sysInfos    String array with system information
     * @param p_dxramInfos  String array with dxram information
     */
    public MonitoringSysInfoMessage(short p_destination, String[] p_sysInfos, String[] p_dxramInfos, boolean p_isPageCacheInUse) {
        super(p_destination, DXRAMMessageTypes.MONITORING_MESSAGES_TYPE,
                MonitoringMessages.SUBTYPE_MONITORING_SYS_INFO);

        m_sysInfos = p_sysInfos;
        m_dxramInfos = p_dxramInfos;
        m_isPageCacheInUse = p_isPageCacheInUse;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeInt(m_sysInfos.length);

        for (String info : m_sysInfos) {
            p_exporter.writeString(info);
        }

        p_exporter.writeInt(m_dxramInfos.length);

        for (String info : m_dxramInfos) {
            p_exporter.writeString(info);
        }

        p_exporter.writeBoolean(m_isPageCacheInUse);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        int length = p_importer.readInt(5);
        m_sysInfos = new String[length];

        for (int i = 0; i < length; i++) {
            m_sysInfos[i] = p_importer.readString(m_sysInfos[i]);
        }

        length = p_importer.readInt(5);
        m_dxramInfos = new String[length];

        for (int i = 0; i < length; i++) {
            m_dxramInfos[i] = p_importer.readString(m_dxramInfos[i]);
        }

        m_isPageCacheInUse = p_importer.readBoolean(m_isPageCacheInUse);
    }

    @Override
    protected int getPayloadLength() {
        int size = 0;

        for (int i = 0; i < m_sysInfos.length; i++) {
            size += ObjectSizeUtil.sizeofString(m_sysInfos[i]);
        }

        for (int i = 0; i < m_dxramInfos.length; i++) {
            size += ObjectSizeUtil.sizeofString(m_dxramInfos[i]);
        }

        return 2 * Integer.BYTES + size + ObjectSizeUtil.sizeofBoolean();
    }


    /**
     * Returns a wrapper which stores the sys infos.
     */
    public MonitoringSysDxramWrapper getWrapper() {
        return new MonitoringSysDxramWrapper(m_sysInfos, m_dxramInfos, m_isPageCacheInUse);
    }

}
