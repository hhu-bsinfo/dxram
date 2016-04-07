package de.hhu.bsinfo.dxram.engine;

import de.hhu.bsinfo.utils.JNINativeMemory;
import de.hhu.bsinfo.utils.JNIconsole;
import de.hhu.bsinfo.utils.OSValidator;
import de.hhu.bsinfo.utils.log.Logger;

/**
 * Separate class to avoid further bloating of DXRAMEngine to setup JNI related things (used by DXRAMEngine).
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class DXRAMJNIManager 
{
	private final String LOG_HEADER = this.getClass().getSimpleName();
	
	private Logger m_logger = null;
	
	/**
	 * Constructor
	 * @param p_logger Logger so this class can do some logging as well.
	 */
	public DXRAMJNIManager(final Logger p_logger)
	{
		m_logger = p_logger;
	}
	
	/**
	 * Setup JNI related things for DXRAM according to the provided profile via settings.
	 * @param p_settings Settings data for setup.
	 */
	public void setup(final DXRAMEngine.Settings p_settings)
	{
		m_logger.debug(LOG_HEADER, "Setting up JNI classes..." );
		
		String path;
		String cwd = System.getProperty("user.dir");
		String extension = null;
		
		if (OSValidator.isUnix()) {
			extension = "so";
		} else if (OSValidator.isMac()) {
			extension = "dylib";
		} else {
			m_logger.error(LOG_HEADER, "Non supported OS.");
			return;
		}
		
		path = p_settings.getValue("JNI/JNIconsole", String.class);
		if (path == null) {
			path = cwd + "/jni/libJNIconsole." + extension;
		}
		m_logger.debug(LOG_HEADER, "Loading JNIconsole: " + path);
		JNIconsole.load(path);
		
		path = p_settings.getValue("JNI/JNINativeMemory", String.class);
		if (path == null) {
			path = cwd + "/jni/libJNINativeMemory." + extension;
		}
		m_logger.debug(LOG_HEADER, "Loading JNINativeMemory: " + path);
		JNINativeMemory.load(path);
	}

}
