package de.uniduesseldorf.dxram.utils;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;

public class JNIconsole {

	// Statics
	static {
		System.load(Core.getConfiguration().getStringValue(ConfigurationConstants.JNI_CONSOLE_DIRECTORY));
	}

	// Methods
	/**
	 * Read one char from console
	 */
	public static native byte[] readline();

}
