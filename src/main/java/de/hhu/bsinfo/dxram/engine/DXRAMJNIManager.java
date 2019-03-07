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

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxutils.OSValidator;

/**
 * Separate class to avoid further bloating of DXRAMEngine to setup JNI related things (used by DXRAMEngine).
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public final class DXRAMJNIManager {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMJNIManager.class);

    private final File m_jniPath;

    /**
     * Constructor
     *
     * @param p_jniPath
     *         Path to root folder with jni libraries
     */
    DXRAMJNIManager(final String p_jniPath) {
        m_jniPath = new File(p_jniPath);

        if (!m_jniPath.exists()) {
            LOGGER.warn("JNI root directory %s does not exist, creating...", m_jniPath.getAbsolutePath());

            if (!m_jniPath.mkdir()) {
                throw new DXRAMRuntimeException("Creating JNI root directory " + m_jniPath.getAbsolutePath() +
                        " failed.");
            }
        }
    }

    /**
     * Load a JNI module
     *
     * @param p_module
     *         the module to load.
     * @return True if module loaded, false on error.
     */
    public boolean loadJNIModule(final String p_module) {
        String extension;

        if (OSValidator.isUnix()) {
            extension = ".so";
        } else if (OSValidator.isMac()) {
            extension = ".dylib";
        } else {
            LOGGER.error("Non supported OS for module %s", p_module);
            return false;
        }

        // Load JNI-lib for given module
        File module = new File(m_jniPath.getAbsolutePath() + "/lib" + p_module + extension);

        if (!module.exists()) {
            LOGGER.error("Failed to load module %s from %s, does not exist", p_module, module.getAbsolutePath());
            return false;
        }

        LOGGER.info("Loading module %s: %s", p_module, module.getAbsolutePath());

        try {
            System.load(module.getAbsolutePath());
        } catch (final Throwable e) {
            LOGGER.error("Loading module %s from %s failed: %s", p_module, module.getAbsolutePath(), e.getMessage());
            return false;
        }

        return true;
    }
}
