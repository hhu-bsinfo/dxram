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
package de.hhu.bsinfo.dxram.monitoring;

/**
 * Wrapper class which provides information about DXRAM (compile type, git commit, ...)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 14.07.2018
 */
public class MonitoringDXRAMInformation {
    private static String ms_buildDate;
    private static String ms_buildUser;
    private static String ms_buildType;

    private static String ms_version;
    private static String ms_commit;

    private static boolean ms_pageCacheInUse;

    /**
     * Sets the values of class.
     *
     * @param p_buildDate Build Date
     * @param p_buildUser Build user
     * @param p_buildType Build Type (Release, Debug,...)
     * @param p_version   DXRAM Version
     * @param p_commit    Commit Hash
     * @param p_pageCache page cache enabled ?
     */
    public static void setValues(final String p_buildDate, final String p_buildUser, final String p_buildType,
                                 final String p_version, final String p_commit, final boolean p_pageCache) {
        ms_buildDate = p_buildDate;
        ms_buildUser = p_buildUser;
        ms_buildType = p_buildType;

        ms_version = p_version;
        ms_commit = p_commit;

        ms_pageCacheInUse = p_pageCache;
    }

    /**
     * Returns build date
     */
    public static String getBuildDate() {
        return ms_buildDate;
    }

    /**
     * Returns name of user who build dxram.
     */
    public static String getBuildUser() {
        return ms_buildUser;
    }

    /**
     * Returns Build Type as String.
     */
    public static String getBuildType() {
        return ms_buildType;
    }

    /**
     * Returns DXRAM Version
     */
    public static String getVersion() {
        return ms_version;
    }

    /**
     * Returns Commit number.
     */
    public static String getCommit() {
        return ms_commit;
    }

    /**
     * Returns true if page cache is enabled (or in use).
     */
    public static boolean isPageCacheInUse() {
        return ms_pageCacheInUse;
    }

    /**
     * Private construcotr
     */
    private MonitoringDXRAMInformation() {
    }
}

