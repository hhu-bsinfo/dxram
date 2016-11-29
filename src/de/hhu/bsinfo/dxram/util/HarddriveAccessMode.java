/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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

package de.hhu.bsinfo.dxram.util;

/**
 * Represents the harddrive access modes.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 25.11.2016
 */
public enum HarddriveAccessMode {
    RANDOM_ACCESS_FILE, ODIRECT, RAW_DEVICE;

    private static final String RANDOM_ACCESS_FILE_STR = "raf";
    private static final String ODIRECT_STR = "dir";
    private static final String RAW_DEVICE_STR = "raw";

    /**
     * Get the harddrive access mode from a full string.
     *
     * @param p_str
     *     String to parse.
     * @return HarddriveAccess
     */
    public static HarddriveAccessMode convert(final String p_str) {
        String str = p_str.toLowerCase();
        switch (str) {
            case RANDOM_ACCESS_FILE_STR:
                return RANDOM_ACCESS_FILE;
            case ODIRECT_STR:
                return ODIRECT;
            default:
                return RAW_DEVICE;
        }
    }

    /**
     * Returns the JNI file name
     *
     * @param p_mode
     *     the HarddriveAccessMode
     * @return the name
     */
    public static String getJNIFileName(final HarddriveAccessMode p_mode) {
        if (p_mode == RANDOM_ACCESS_FILE) {
            return "";
        } else if (p_mode == ODIRECT) {
            return "JNIFileDirect";
        } else {
            return "JNIFileRaw";
        }
    }
}
