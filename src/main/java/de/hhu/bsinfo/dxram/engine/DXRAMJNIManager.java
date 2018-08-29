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

package de.hhu.bsinfo.dxram.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.OSValidator;

/**
 * Separate class to avoid further bloating of DXRAMEngine to setup JNI related things (used by DXRAMEngine).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class DXRAMJNIManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMJNIManager.class.getSimpleName());

    private static String ms_jniPath;

    /**
     * Constructor
     */
    private DXRAMJNIManager() {
    }

    /**
     * Setup JNI related things for DXRAM according to the provided profile via settings.
     *
     * @param p_module
     *         the module to load.
     */
    public static void loadJNIModule(final String p_module) {

        LOGGER.debug("Setting up JNI class for %s", p_module);

        String path;
        final String cwd = System.getProperty("user.dir");
        String extension;

        if (OSValidator.isUnix()) {
            extension = ".so";
        } else if (OSValidator.isMac()) {
            extension = ".dylib";
        } else {

            LOGGER.error("Non supported OS");

            return;
        }

        // Load JNI-lib for given module
        path = cwd + '/' + ms_jniPath + "/lib" + p_module + extension;

        LOGGER.debug("Loading %s: %s", p_module, path);

        System.load(path);
    }

    /**
     * Setup JNI related things for DXRAM according to the provided profile via settings.
     *
     * @param p_engineConfig
     *         EngineConfig data for setup.
     */
    public static void setup(final DXRAMContext.EngineConfig p_engineConfig) {
        ms_jniPath = p_engineConfig.getJniPath();
    }

}
