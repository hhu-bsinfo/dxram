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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handler for DXRAM context, loading configuration, creating default configuration, configuration overriding
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 24.10.2016
 */
class DXRAMContextHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMContextHandler.class.getSimpleName());

    private static final String DXRAM_CONFIG_FILE_PATH = "config/dxram.json";

    private final DXRAMComponentManager m_componentManager;
    private final DXRAMServiceManager m_serviceManager;
    private DXRAMContext m_context = new DXRAMContext();

    /**
     * Constructor
     *
     * @param p_componentManager
     *         the DXRAMComponentManager
     * @param p_serviceManager
     *         the DXRAMServiceManager
     */
    DXRAMContextHandler(final DXRAMComponentManager p_componentManager, final DXRAMServiceManager p_serviceManager) {
        m_componentManager = p_componentManager;
        m_serviceManager = p_serviceManager;
    }

    /**
     * Get the DXRAM context
     *
     * @return DXRAM context
     */
    DXRAMContext getContext() {
        return m_context;
    }

    /**
     * Create a default configuration file
     *
     * @param p_configFilePath
     *         Path for configuration file
     * @return True if creating config file successful, false otherwise
     */
    boolean createDefaultConfiguration(final String p_configFilePath) {
        LOGGER.info("No valid configuration found or specified via vm argument -Ddxram.config, creating default configuration '%s'...", p_configFilePath);

        String configFilePath;

        if (p_configFilePath.isEmpty()) {
            configFilePath = DXRAM_CONFIG_FILE_PATH;
        } else {
            configFilePath = p_configFilePath;
        }

        File file = new File(configFilePath);
        if (file.exists()) {
            if (!file.delete()) {
                LOGGER.error("Deleting existing config file %s failed", file);
                return false;
            }
        }

        try {
            if (!file.createNewFile()) {
                LOGGER.error("Creating new config file %s failed", file);
                return false;
            }
        } catch (final IOException e) {
            LOGGER.error("Creating new config file %s failed: %s", file, e.getMessage());
            return false;
        }

        m_context = new DXRAMContext();
        m_context.createDefaultComponents(m_componentManager);
        m_context.createDefaultServices(m_serviceManager);

        Gson gson = DXRAMGsonContext.createGsonInstance();
        String jsonString = gson.toJson(m_context);

        try {
            PrintWriter writer = new PrintWriter(file);
            writer.print(jsonString);
            writer.close();
        } catch (final FileNotFoundException e) {
            // we can ignored this here, already checked that
        }

        return true;
    }

    /**
     * Load an existing configuration
     *
     * @param p_configFilePath
     *         Path to existing configuration file
     * @return True if loading successful, false on error
     */
    boolean loadConfiguration(final String p_configFilePath) {
        LOGGER.info("Loading configuration '%s'...", p_configFilePath);

        Gson gson = DXRAMGsonContext.createGsonInstance();

        JsonElement element;
        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
        } catch (final Exception e) {
            LOGGER.error("Could not load configuration '%s': %s", p_configFilePath, e.getMessage());
            return false;
        }

        if (element == null) {
            LOGGER.error("Could not load configuration '%s': empty configuration file", p_configFilePath);
            return false;
        }

        overrideConfigurationWithVMArguments(element.getAsJsonObject());

        try {
            m_context = gson.fromJson(element, DXRAMContext.class);
        } catch (final Exception e) {
            LOGGER.error("Loading configuration '%s' failed: %s", p_configFilePath, e.getMessage());
            return false;
        }

        if (m_context == null) {
            LOGGER.error("Loading configuration '%s' failed: context null", p_configFilePath);
            return false;
        }

        // verify configuration values
        if (!m_context.verifyConfigurationValuesComponents()) {
            return false;
        }

        if (!m_context.verifyConfigurationValuesComponents()) {
            return false;
        }

        // create component/service instances
        m_context.createComponentsFromConfig(m_componentManager, m_context.getConfig().getEngineConfig().getRole());
        m_context.createServicesFromConfig(m_serviceManager, m_context.getConfig().getEngineConfig().getRole());

        return true;
    }

    /**
     * Override current configuration with further values provided via VM arguments
     *
     * @param p_object
     *         Root object of JSON configuration tree
     */
    private static void overrideConfigurationWithVMArguments(final JsonObject p_object) {
        Properties props = System.getProperties();
        Enumeration e = props.propertyNames();

        while (e.hasMoreElements()) {

            String key = (String) e.nextElement();
            if (key.startsWith("dxram.") && !"dxram.config".equals(key)) {

                String[] tokens = key.split("\\.");

                JsonObject parent = p_object;
                JsonObject child = null;
                // skip dxram token
                for (int i = 1; i < tokens.length; i++) {

                    JsonElement elem;

                    // support access to arrays/maps
                    if (tokens[i].contains("[")) {
                        String[] arrayTokens = tokens[i].split("\\[");
                        // trim ]
                        arrayTokens[1] = arrayTokens[1].substring(0, arrayTokens[1].length() - 1);

                        JsonElement elemArray = parent.get(arrayTokens[0]);
                        elem = elemArray.getAsJsonObject().get(arrayTokens[1]);
                    } else {
                        elem = parent.get(tokens[i]);
                    }

                    // if first element is already invalid
                    if (elem == null) {
                        break;
                    }

                    if (elem.isJsonObject()) {
                        child = elem.getAsJsonObject();
                    } else if (i + 1 == tokens.length) {
                        break;
                    }

                    if (child == null) {
                        break;
                    }

                    parent = child;
                }

                String propertyKey = props.getProperty(key);

                // try to determine type, not a very nice way =/
                if (propertyKey.matches("^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$")) {
                    // ip address
                    parent.addProperty(tokens[tokens.length - 1], propertyKey);
                } else if (propertyKey.matches("[-+]?\\d*\\.?\\d+")) {
                    // numeric
                    parent.addProperty(tokens[tokens.length - 1], Long.parseLong(propertyKey));
                } else {
                    // string
                    parent.addProperty(tokens[tokens.length - 1], propertyKey);
                }
            }
        }
    }
}
