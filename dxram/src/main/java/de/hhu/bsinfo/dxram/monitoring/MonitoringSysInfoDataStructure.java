package de.hhu.bsinfo.dxram.monitoring;

import java.util.Arrays;

import de.hhu.bsinfo.dxmonitor.state.SystemState;
import de.hhu.bsinfo.dxram.data.DataStructure;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

/**
 * Data Structure which stores system information
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.06.2018
 */
public class MonitoringSysInfoDataStructure extends DataStructure {

    private String[] m_sysInfos;
    private String[] m_dxramInfos;

    private boolean m_isPageCacheInUse;

    public MonitoringSysInfoDataStructure() {
        fillValues();
    }

    public MonitoringSysInfoDataStructure(String[] p_sysInfos, String[] p_dxramInfos, boolean p_isPageCacheInUse) {
        m_sysInfos = p_sysInfos;
        m_dxramInfos = p_dxramInfos;
        m_isPageCacheInUse = p_isPageCacheInUse;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
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
    public void importObject(Importer p_importer) {
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
    public int sizeofObject() {
        int size = 0;

        for (int i = 0; i < m_sysInfos.length; i++) {
            size += ObjectSizeUtil.sizeofString(m_sysInfos[i]);
        }

        for (int i = 0; i < m_dxramInfos.length; i++) {
            size += ObjectSizeUtil.sizeofString(m_dxramInfos[i]);
        }

        return 2 * Integer.BYTES + size + ObjectSizeUtil.sizeofBoolean();
    }

    @Override
    public String toString() {
        return Arrays.toString(m_sysInfos) + ',' + Arrays.toString(m_dxramInfos) + ',' + m_isPageCacheInUse;
    }

    private void fillValues() {
        m_sysInfos = new String[5];
        m_dxramInfos = new String[5];

        m_sysInfos[0] = SystemState.getKernelVersion();
        m_sysInfos[1] = SystemState.getDistribution();
        m_sysInfos[2] = SystemState.getCurrentWorkingDirectory();
        m_sysInfos[3] = SystemState.getHostName();
        m_sysInfos[4] = SystemState.getUserName();

        m_dxramInfos[0] = MonitoringDXRAMInformation.getBuildUser();
        m_dxramInfos[1] = MonitoringDXRAMInformation.getBuildDate();
        m_dxramInfos[2] = MonitoringDXRAMInformation.getCommit();
        m_dxramInfos[3] = MonitoringDXRAMInformation.getVersion();
        m_dxramInfos[4] = MonitoringDXRAMInformation.getBuildType();
        m_isPageCacheInUse = MonitoringDXRAMInformation.isPageCacheInUse();
    }

    public String getKernelVersion() {
        return m_sysInfos[0];
    }

    public String getDistribution() {
        return m_sysInfos[1];
    }

    public String getCWD() {
        return m_sysInfos[2];
    }

    public String getHostName() {
        return m_sysInfos[3];
    }

    public String getLoggedInUser() {
        return m_sysInfos[4];
    }

    public String getBuildUser() {
        return m_dxramInfos[0];
    }

    public String getBuildDate() {
        return m_dxramInfos[1];
    }

    public String getDxramCommit() {
        return m_dxramInfos[2];
    }

    public String getDxramVersion() {
        return m_dxramInfos[3];
    }

    public String getBuildType() {
        return m_dxramInfos[4];
    }

    public boolean isPageCacheInUse() {
        return m_isPageCacheInUse;
    }
}
