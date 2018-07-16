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

package de.hhu.bsinfo.dxram.monitoring.util;

/**
 * DXRAM and system information wrapper class
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringSysDxramWrapper {

    private String[] m_sysInfos;
    private String[] m_dxramInfos;

    private boolean m_isPageCacheInUse;

    /**
     * Constructor
     *
     * @param p_sysInfos         list of system information (Hostname, cwd, ...)
     * @param p_dxramInfos       list of dxram information (Build Date, Version, User)
     * @param p_isPageCacheInUse
     */
    public MonitoringSysDxramWrapper(String[] p_sysInfos, String[] p_dxramInfos, boolean p_isPageCacheInUse) {
        m_sysInfos = p_sysInfos;
        m_dxramInfos = p_dxramInfos;
        m_isPageCacheInUse = p_isPageCacheInUse;
    }

    /**
     * Returns kernel version.
     */
    public String getKernelVersion() {
        return m_sysInfos[0];
    }

    /**
     * Returns distribution name.
     */
    public String getDistribution() {
        return m_sysInfos[1];
    }

    /**
     * Returns current working directory.
     */
    public String getCWD() {
        return m_sysInfos[2];
    }

    /**
     * Returns hostname.
     */
    public String getHostName() {
        return m_sysInfos[3];
    }

    /**
     * Returns name of logged in user.
     */
    public String getLoggedInUser() {
        return m_sysInfos[4];
    }

    /**
     * Returns name of user who build dxram.
     */
    public String getBuildUser() {
        return m_dxramInfos[0];
    }

    /**
     * Returns build date.
     */
    public String getBuildDate() {
        return m_dxramInfos[1];
    }

    /**
     * Returns dxram commit hash.
     */
    public String getDxramCommit() {
        return m_dxramInfos[2];
    }

    /**
     * Returns dxram version.
     */
    public String getDxramVersion() {
        return m_dxramInfos[3];
    }

    /**
     * Returns build type.
     */
    public String getBuildType() {
        return m_dxramInfos[4];
    }

    /**
     * Returns true if page cache is used.
     */
    public boolean isPageCacheInUse() {
        return m_isPageCacheInUse;
    }
}
