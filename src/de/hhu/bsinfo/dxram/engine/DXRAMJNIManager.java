
package de.hhu.bsinfo.dxram.engine;

import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.OSValidator;
import de.hhu.bsinfo.utils.logger.Logger;

/**
 * Separate class to avoid further bloating of DXRAMEngine to setup JNI related things (used by DXRAMEngine).
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class DXRAMJNIManager {
	private static final String LOG_HEADER = DXRAMJNIManager.class.getSimpleName();

	private Logger m_logger;

	/**
	 * Constructor
	 *
	 * @param p_logger Logger so this class can do some logging as well.
	 */
	public DXRAMJNIManager(final Logger p_logger) {
		m_logger = p_logger;
	}

	/**
	 * Setup JNI related things for DXRAM according to the provided profile via settings.
	 *
	 * @param p_settings Settings data for setup.
	 */
	public void setup(final DXRAMEngine.Settings p_settings) {
		// #if LOGGER >= DEBUG
		m_logger.debug(LOG_HEADER, "Setting up JNI classes...");
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
			m_logger.error(LOG_HEADER, "Non supported OS.");
			// #endif /* LOGGER >= ERROR */
			return;
		}

		path = p_settings.getValue("JNI/JNIconsole", String.class);
		if (path == null) {
			path = cwd + "/jni/libJNIconsole." + extension;
		}
		// #if LOGGER >= DEBUG
		m_logger.debug(LOG_HEADER, "Loading JNIconsole: " + path);
		// #endif /* LOGGER >= DEBUG */

		JNIconsole.load(path);

		//		path = p_settings.getValue("JNI/JNINativeMemory", String.class);
		//		if (path == null) {
		//			path = cwd + "/jni/libJNINativeMemory." + extension;
		//		}
		//		// #if LOGGER >= DEBUG
		//		m_logger.debug(LOG_HEADER, "Loading JNINativeMemory: " + path);
		//		// #endif /* LOGGER >= DEBUG */
		//
		//		JNINativeMemory.load(path);
	}

}
