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

package de.hhu.bsinfo.utils;

/**
 * Check which operating system the application is running on.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 04.04.2016
 */
public final class OSValidator {

    private static String ms_os = System.getProperty("os.name").toLowerCase();

    public static final String MS_WINDOWS = "win";
    public static final String MS_OSX = "osx";
    public static final String MS_UNIX = "uni";
    public static final String MS_SOLARIS = "sol";
    public static final String MS_UNKNOWN_OS = "unkn";

    /**
     * Utils class
     */
    private OSValidator() {
    }

    /**
     * Check if running on windows.
     *
     * @return True if windows, false otherwise.
     */
    public static boolean isWindows() {
        return ms_os.contains("win");
    }

    /**
     * Check if running on mac osx.
     *
     * @return True if mac osx, false otherwise.
     */
    public static boolean isMac() {
        return ms_os.contains("mac");
    }

    /**
     * Check if running on unix like systems.
     *
     * @return True if unix like system, false otherwise.
     */
    public static boolean isUnix() {
        return ms_os.contains("nix") || ms_os.contains("nux") || ms_os.indexOf("aix") > 0;
    }

    /**
     * Check if running on solaris.
     *
     * @return True if solaris, false otherwise.
     */
    public static boolean isSolaris() {
        return ms_os.contains("sunos");
    }

    /**
     * Get the string of the system running on.
     *
     * @return String of system running on (refer to final statics of this class).
     */
    public static String getOS() {
        if (isWindows()) {
            return "win";
        } else if (isMac()) {
            return "osx";
        } else if (isUnix()) {
            return "uni";
        } else if (isSolaris()) {
            return "sol";
        } else {
            return "unkn";
        }
    }
}
