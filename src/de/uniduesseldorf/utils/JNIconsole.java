
package de.uniduesseldorf.utils;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;

/**
 * Wrapper for accessing GNU readline lib
 * @author Michael Schoettner 07.09.2015
 */
public final class JNIconsole {

	/**
	 * Constructor
	 */
	private JNIconsole() {}

	// Statics
	static {
		System.load(Core.getConfiguration().getStringValue(ConfigurationConstants.JNI_CONSOLE_DIRECTORY));
	}

	// Methods

	/**
	 * Read one line from console.
	 * @return the read line
	 */
	public static native byte[] readline();

}
