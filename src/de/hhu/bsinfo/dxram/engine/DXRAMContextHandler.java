package de.hhu.bsinfo.dxram.engine;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Properties;

/**
 * Handler for DXRAM context, loading configuration, creating default configuration, configuration overriding
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 24.10.16
 */
class DXRAMContextHandler {

    private static final String DXRAM_CONFIG_FILE_PATH = "config/dxram.json";

    private DXRAMContext m_context = new DXRAMContext();

    /**
     * Constructor
     */
    DXRAMContextHandler() {

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
     * @param p_configFilePath Path for configuration file
     * @return True if creating config file successful, false otherwise
     */
    boolean createDefaultConfiguration(final String p_configFilePath) {

        System.out.println("No valid configuration found or specified via vm argument -Ddxram.config, "
                + "creating default configuration '" + p_configFilePath + "'...");

        String configFilePath;

        if (p_configFilePath.isEmpty()) {
            configFilePath = DXRAM_CONFIG_FILE_PATH;
        } else {
            configFilePath = p_configFilePath;
        }

        File file = new File(configFilePath);
        if (file.exists()) {
            if (!file.delete()) {
                System.out.println("Deleting existing config file " + file + " failed");
                return false;
            }
        }

        try {
            if (!file.createNewFile()) {
                System.out.println("Creating new config file " + file + " failed");
                return false;
            }
        } catch (final IOException e) {
            System.out.println("Creating new config file " + file + " failed: " + e.getMessage());
            return false;
        }

        // create default components and services
        DXRAMComponentManager.registerDefault();
        DXRAMServiceManager.registerDefault();

        m_context = new DXRAMContext();
        m_context.fillDefaultComponents();
        m_context.fillDefaultServices();

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
     * @param p_configFilePath Path to existing configuration file
     * @return True if loading successful, false on error
     */
    boolean loadConfiguration(final String p_configFilePath) {

        System.out.println("Loading configuration '" + p_configFilePath + "'...");

        Gson gson = DXRAMGsonContext.createGsonInstance();

        JsonElement element;
        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
        } catch (final Exception e) {
            System.out.println("Could not load configuration '" + p_configFilePath + "': " + e.getMessage());
            return false;
        }

        overrideConfigurationWithVMArguments(element.getAsJsonObject());

        m_context = gson.fromJson(element, DXRAMContext.class);

        return true;
    }

    /**
     * Override current configuration with further values provided via VM arguments
     *
     * @param p_object Root object of JSON configuration tree
     */
    private void overrideConfigurationWithVMArguments(final JsonObject p_object) {

        Properties props = System.getProperties();
        Enumeration e = props.propertyNames();

        while (e.hasMoreElements()) {

            String key = (String) e.nextElement();
            if (key.startsWith("dxram.") && !key.equals("dxram.config")) {

                String[] tokens = key.split("\\.");

                JsonObject parent = p_object;
                JsonObject child = null;
                // skip dxram token
                for (int i = 1; i < tokens.length; i++) {

                    JsonElement elem = parent.get(tokens[i]);

                    // if first element is already invalid
                    if (elem == null) {
                        child = null;
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

                if (child == null) {
                    System.out.println("Invalid vm argument '" + key + "'");
                    continue;
                }

                String propertyKey = props.getProperty(key);

                // try to determine type, not a very nice way =/
                if (propertyKey.matches(
                        "^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
                                + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
                                + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\."
                                + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$")) {
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
