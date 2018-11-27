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
            LogManager.getFormatterLogger(DXRAMConfigBuilderJsonFile2.class.getSimpleName());

    private static final String JVM_ARG_KEY = "dxram.config";
    private static final String DEFAULT_PATH = "config/dxram.json";
    private static final String PATH;

    static {
        String value = System.getProperty(JVM_ARG_KEY);

        if (value == null) {
            PATH = DEFAULT_PATH;
            LOGGER.warn("No config file path specified as JVM args (%s), use default path: %s", JVM_ARG_KEY, PATH);
        } else {
            PATH = value;
            LOGGER.info("JVM arg config path: %s", PATH);
        }
    }

    /**
     * Constructor
     */
    public DXRAMConfigBuilderJsonFile2() {
        super (PATH, true);
    }
}
