package de.hhu.bsinfo.dxram.monitoring.util;

import de.hhu.bsinfo.dxmonitor.state.SystemState;
import de.hhu.bsinfo.dxram.monitoring.MonitoringDXRAMInformation;

/**
 * DXRAM and system information wrapper class
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 12.07.2018
 */
public class MonitoringSysDxramWrapper {

    private String[] m_sysInfos;
    private String[] m_dxramInfos;

    private boolean m_isPageCacheInUse;

    public MonitoringSysDxramWrapper(String[] p_sysInfos, String[] p_dxramInfos, boolean p_isPageCacheInUse) {
        m_sysInfos = p_sysInfos;
        m_dxramInfos = p_dxramInfos;
        m_isPageCacheInUse = p_isPageCacheInUse;
    }

    private void fillSysInfos(String[] p_sysInfos) {
        m_sysInfos = p_sysInfos;
    }


    private void fillDxramInfos(String[] p_dxramInfos) {
        m_dxramInfos = p_dxramInfos;
    }

    public void fillPageCacheInfo(boolean p_isPageCacheInUse) {
        m_isPageCacheInUse = p_isPageCacheInUse;
    }

    private void fillValues(String[] p_sysInfos, String[] p_dxramInfos, boolean p_isPageCacheInUse) {
        m_sysInfos = p_sysInfos;
        m_dxramInfos = p_dxramInfos;
        m_isPageCacheInUse = p_isPageCacheInUse;
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
