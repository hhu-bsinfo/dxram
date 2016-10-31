
package de.hhu.bsinfo.dxram.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.OSValidator;

/**
 * Separate class to avoid further bloating of DXRAMEngine to setup JNI related things (used by DXRAMEngine).
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 03.02.2016
 */
public class DXRAMJNIManager {

    private static final Logger LOGGER = LogManager.getFormatterLogger(DXRAMJNIManager.class.getSimpleName());

    /**
     * Constructor
     */
    public DXRAMJNIManager() {
    }

    /**
     * Setup JNI related things for DXRAM according to the provided profile via settings.
     * @param p_engineSettings
     *            EngineSettings data for setup.
     */
    public void setup(final DXRAMContext.EngineSettings p_engineSettings) {
        // #if LOGGER >= DEBUG
        LOGGER.debug("Setting up JNI classes...");
        // #endif /* LOGGER >= DEBUG */

        String path;
        final String cwd = System.getProperty("user.dir");
        String extension = null;

        if (OSValidator.isUnix()) {
            extension = "so";
        } else if (OSValidator.isMac()) {
            extension = "dylib";
        } else {
            // #if LOGGER >= ERROR
            LOGGER.error("Non supported OS");
            // #endif /* LOGGER >= ERROR */
            return;
        }

        path = cwd + "/" + p_engineSettings.getJNIPath() + "/libJNIconsole." + extension;

        // #if LOGGER >= DEBUG
        LOGGER.debug("Loading JNIconsole: " + path);
        // #endif /* LOGGER >= DEBUG */

        JNIconsole.load(path);
    }

}
