package de.hhu.bsinfo.dxram.monitoring;

/**
 * Wrapper class which provides information about DXRAM (compile type, git commit, ...)
 *
 * @author Burak Akguel, burak.akguel@hhu.de, 08.06.2018
 */
public class MonitoringDXRAMInformation {
    private static String ms_buildDate;
    private static String ms_buildUser;
    private static String ms_buildType;

    private static String ms_version;
    private static String ms_commit;

    private static boolean ms_pageCacheInUse;

    public static void setValues(final String p_buildDate, final String p_buildUser, final String p_buildType,
            final String p_version, final String p_commit, final boolean p_pageCache) {
        ms_buildDate = p_buildDate;
        ms_buildUser = p_buildUser;
        ms_buildType = p_buildType;

        ms_version = p_version;
        ms_commit = p_commit;

        ms_pageCacheInUse = p_pageCache;
    }

    public static String getBuildDate() {
        return ms_buildDate;
    }

    public static String getBuildUser() {
        return ms_buildUser;
    }

    public static String getBuildType() {
        return ms_buildType;
    }

    public static String getVersion() {
        return ms_version;
    }

    public static String getCommit() {
        return ms_commit;
    }

    public static boolean isPageCacheInUse() {
        return ms_pageCacheInUse;
    }

    private MonitoringDXRAMInformation() {
    }

}

