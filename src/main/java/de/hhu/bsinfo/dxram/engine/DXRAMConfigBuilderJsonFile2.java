package de.hhu.bsinfo.dxram.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a simplified version based on the JSON file reader which uses a default path and always creates a
 * config file if one doesn't exist, yet.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.11.2018
 */
public class DXRAMConfigBuilderJsonFile2 extends DXRAMConfigBuilderJsonFile {
    private static final Logger LOGGER =
            LogManager.getFormatterLogger(DXRAMConfigBuilderJsonFile2.class);

    private static final String JVM_ARG_CREATE_EXIT = "dxram.config.createDefaultAndExit";
    private static final String JVM_ARG_KEY = "dxram.config";
    private static final String DEFAULT_PATH = "config/dxram.json";
    private static final String PATH;
    private static final boolean CREATE_AND_EXIT;

    static {
        String value = System.getProperty(JVM_ARG_KEY);

        if (value == null) {
            PATH = DEFAULT_PATH;
            LOGGER.warn("No config file path specified as JVM args (%s), use default path: %s", JVM_ARG_KEY, PATH);
        } else {
            PATH = value;
        }

        CREATE_AND_EXIT = System.getProperty(JVM_ARG_CREATE_EXIT) != null;
    }

    /**
     * Constructor
     */
    public DXRAMConfigBuilderJsonFile2() {
        super (PATH, true, CREATE_AND_EXIT);
    }
}
