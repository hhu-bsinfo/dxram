package de.hhu.bsinfo.dxram.engine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Properties;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import de.hhu.bsinfo.dxutils.JsonUtil;
import de.hhu.bsinfo.dxutils.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The default context creator which loads a configuration from a file, creates a default configuration and writes
 * it back to a file if none exists and handles config parameter overriding using JVM arguments
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.08.2017
 */
public class DXRAMContextCreatorFile implements DXRAMContextCreator {
    private static final Logger LOGGER = LogManager.getFormatterLogger(
            DXRAMContextCreatorFile.class.getSimpleName());

    private static final String DXRAM_CONFIG_FILE_PATH = "config/dxram.json";

    /**
     * Constructor
     */
    public DXRAMContextCreatorFile() {

    }

    @Override
    public DXRAMContext create(final DXRAMComponentManager p_componentManager,
            final DXRAMServiceManager p_serviceManager) {
        // check vm arguments for configuration override
        String config = System.getProperty("dxram.config");

        if (config == null) {
            config = DXRAM_CONFIG_FILE_PATH;
        }

        LOGGER.info("Loading configuration file: %s", config);

        // check if a config needs to be created
        if (!new File(config).exists()) {
            createDefaultConfiguration(config, p_componentManager, p_serviceManager);

            LOGGER.info("Default configuration created (%s), please restart DXRAM", config);
            return null;
        }

        return loadConfiguration(config);
    }

    /**
     * Create a default configuration file
     *
     * @param p_configFilePath
     *         Path for configuration file
     */
    private static void createDefaultConfiguration(final String p_configFilePath,
            final DXRAMComponentManager p_componentManager, final DXRAMServiceManager p_serviceManager) {
        String configFilePath;

        if (p_configFilePath.isEmpty()) {
            configFilePath = DXRAM_CONFIG_FILE_PATH;
        } else {
            configFilePath = p_configFilePath;
        }

        LOGGER.info("No valid configuration found or specified via vm argument -Ddxram.config, creating default " +
                "configuration in '%s'...", configFilePath);

        File file = new File(configFilePath);

        if (file.exists()) {
            if (!file.delete()) {
                LOGGER.error("Deleting existing config file %s failed", file);
                return;
            }
        }

        try {
            if (!file.createNewFile()) {
                LOGGER.error("Creating new config file %s failed", file);
                return;
            }
        } catch (final IOException e) {
            LOGGER.error("Creating new config file %s failed: %s", file, e.getMessage());
            return;
        }

        DXRAMContext context = new DXRAMContext();
        context.createDefaultComponents(p_componentManager);
        context.createDefaultServices(p_serviceManager);

        Gson gson = DXRAMGsonContext.createGsonInstance();
        String jsonString = gson.toJson(context);

        try {
            PrintWriter writer = new PrintWriter(file);
            writer.print(jsonString);
            writer.close();
        } catch (final FileNotFoundException e) {
            // we can ignored this here, already checked that
        }
    }

    /**
     * Load an existing configuration
     *
     * @param p_configFilePath
     *         Path to existing configuration file
     * @return DXRAMContext instance on success, null on failure
     */
    private static DXRAMContext loadConfiguration(final String p_configFilePath) {
        LOGGER.info("Loading configuration '%s'...", p_configFilePath);

        Gson gson = DXRAMGsonContext.createGsonInstance();

        JsonElement element;

        try {
            element = gson.fromJson(new String(Files.readAllBytes(Paths.get(p_configFilePath))), JsonElement.class);
        } catch (final Exception e) {
            LOGGER.error("Could not load configuration '%s': %s", p_configFilePath, e.getMessage());
            return null;
        }

        if (element == null) {
            LOGGER.error("Could not load configuration '%s': empty configuration file", p_configFilePath);
            return null;
        }

        JsonUtil.override(element, System.getProperties(), "dxram.", Collections.singletonList("dxram.config"));

        //overrideConfigurationWithVMArguments(element.getAsJsonObject());

        LOGGER.info(element.getAsJsonObject().toString());

        DXRAMContext context;

        try {
            context = gson.fromJson(element, DXRAMContext.class);
        } catch (final Exception e) {
            LOGGER.error("Loading configuration '%s' failed: %s", p_configFilePath, e.getMessage());
            return null;
        }

        if (context == null) {
            LOGGER.error("Loading configuration '%s' failed: context null", p_configFilePath);
            return null;
        }

        return context;
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

                        LOGGER.info("Reading %s[%s]", arrayTokens[0], arrayTokens[1]);
                        // Differentiate between arrays and maps
                        if (StringUtils.isNumeric(arrayTokens[1])) {
                            LOGGER.info("Found numeric index at %s", arrayTokens[0]);
                            elem = getOrCreateArrayObject(elemArray, Integer.parseInt(arrayTokens[1]));
                        } else {
                            elem = elemArray.getAsJsonObject().get(arrayTokens[1]);
                        }
                    } else {
                        elem = parent.get(tokens[i]);
                    }

                    // if first element is already invalid
                    if (elem == null) {
                        break;
                    }

                    if (elem.isJsonObject() || elem.isJsonArray()) {
                        child = elem.getAsJsonObject();
                    } else if (i + 1 == tokens.length) {
                        break;
                    }

                    if (child == null) {
                        break;
                    }

                    parent = child;
                }

                String jsonKey = tokens[tokens.length - 1];
                String jsonValue = props.getProperty(key);
                LOGGER.info("Setting %s to %s", jsonKey, jsonValue);

                // try to determine type, not a very nice way =/
                if (jsonValue.matches("^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." + "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
                        "([01]?\\d\\d?|2[0-4]\\d|25[0-5])$")) {
                    // ip address
                    parent.addProperty(jsonKey, jsonValue);
                } else if (jsonValue.matches("[-+]?\\d*\\.?\\d+")) {
                    // numeric
                    parent.addProperty(jsonKey, Long.parseLong(jsonValue));
                } else {
                    // string
                    parent.addProperty(jsonKey, jsonValue);
                }
            }
        }
    }

    private static JsonObject getOrCreateArrayObject(final JsonElement p_parent, final int p_index) {
        JsonArray array =  p_parent.getAsJsonArray();

        int size = array.size();
        if (p_index > size) {
            throw new IndexOutOfBoundsException();
        }

        JsonObject object;
        if (p_index == size) {
            LOGGER.info("Creating new autostart entry");
            object = new JsonObject();
            array.add(object);
            LOGGER.info(array.toString());
        } else {
            LOGGER.info("Reusing old autostart entry");
            object = array.get(p_index).getAsJsonObject();
        }

        return object;
    }
}
