/*
 * Copyright (C) 2018 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxmonitor.component;


import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * This class can be used to get system information.
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 23.11.17
 */
public final class SysInfoComponent {

    private static String ms_kernelVersion;
    private static String ms_distribution;
    private static String ms_cwd;
    private static String ms_hostName;
    private static String ms_userLoggedIn;
    private static boolean ms_pageCacheInUse;
    // DXRAM-specific
    private static String ms_buildDate;
    private static String ms_buildUser;
    private static String ms_dxramCommit;
    private static String ms_dxramVersion;

    private SysInfoComponent() {}

    public static void setupValues(String p_buildDate, String p_buildUser, String p_commit, String p_version, boolean p_pageCache) {
        ms_buildDate = p_buildDate;
        ms_buildUser = p_buildUser;
        ms_dxramCommit = p_commit;
        ms_dxramVersion = p_version;

        ms_pageCacheInUse = p_pageCache;
        try {
            ms_hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        ms_cwd = System.getProperty("user.dir");
        ms_distribution = System.getProperty("os.name");
        ms_kernelVersion = System.getProperty("os.version");
        ms_userLoggedIn = System.getProperty("user.name");
    }


    public static String getKernelVersion() {
        return ms_kernelVersion;
    }

    public static String getDistribution() {
        return ms_distribution;
    }

    public static String getCurrentWorkingDirectory() {
        return ms_cwd;
    }

    public static String getHostName() {
        return ms_hostName;
    }

    public static String getLoggedInUser() {
        return ms_userLoggedIn;
    }

    public static boolean isPageCacheInUse() {
        return ms_pageCacheInUse;
    }

    public static String getBuildDate() {
        return ms_buildDate;
    }

    public static String getBuildUser() {
        return ms_buildUser;
    }

    public static String getDxramCommit() {
        return ms_dxramCommit;
    }

    public static String getDxramVersion() {
        return ms_dxramVersion;
    }

    /*
    public static MonitoringSysInfoDataStructure getAsDataStructure() {
        String[] sysInfos = new String[]{ms_kernelVersion,ms_distribution,ms_cwd,ms_hostName,ms_userLoggedIn};
        String[] dxramInfos = new String[] {ms_buildUser, ms_buildDate, ms_dxramCommit, ms_dxramVersion};
        return new MonitoringSysInfoDataStructure(sysInfos, dxramInfos, ms_pageCacheInUse);
    }
    */
}
