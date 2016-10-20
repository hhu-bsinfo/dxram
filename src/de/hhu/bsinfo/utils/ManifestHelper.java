
package de.hhu.bsinfo.utils;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

/**
 * Helper class to get information from the manifest file in a jar archive.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 30.03.16
 */
public final class ManifestHelper {
	/**
	 * Utils class
	 */
	private ManifestHelper() {}

	/**
	 * Get the value of a property within the manifest file.
	 * @param p_class
	 *            Target class within the jar package.
	 * @param p_key
	 *            Key for the value to get.
	 * @return If value is found it is returned as a string, null otherwise.
	 */
	public static String getProperty(final Class<?> p_class, final String p_key) {
		String value = null;
		try {
			Enumeration<URL> resources = p_class.getClassLoader().getResources("META-INF/MANIFEST.MF");
			while (resources.hasMoreElements()) {
				Manifest manifest = new Manifest(resources.nextElement().openStream());
				// check that this is your manifest and do what you need or get the next one
				Attributes attr = manifest.getMainAttributes();
				value = attr.getValue(p_key);
				if (value != null) {
					break;
				}
			}
		} catch (final IOException e) {

		}

		return value;
	}
}
