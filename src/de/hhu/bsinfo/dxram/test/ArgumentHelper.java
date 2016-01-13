
package de.hhu.bsinfo.dxram.test;

import java.util.Properties;

/**
 * Helps parsing program arguments
 * @author klein 26.03.2015
 */
public final class ArgumentHelper {

	// Attributes
	private Properties m_arguments;

	// Constructors
	/**
	 * Creates an instance of ArgumentHelper
	 * @param p_arguments
	 *            the program arguments
	 */
	public ArgumentHelper(final String[] p_arguments) {
		m_arguments = new Properties();

		if (p_arguments != null) {
			for (int i = 0; i < p_arguments.length; i++) {
				if (p_arguments[i].startsWith("-")) {
					if (i < p_arguments.length - 1) {
						if (p_arguments[i + 1].startsWith("-")) {
							m_arguments.put(p_arguments[i], "");
						} else {
							m_arguments.put(p_arguments[i], p_arguments[i + 1]);
							i++;
						}
					} else {
						m_arguments.put(p_arguments[i], "");
					}
				}
			}
		}
	}

	// Methods
	/**
	 * Gets the argument value for the given key
	 * @param p_key
	 *            the key
	 * @return the argument
	 */
	public String getArgument(final String p_key) {
		return m_arguments.getProperty(p_key);
	}

	/**
	 * Gets the argument value for the given key. If the key does not exist the default value will be returned
	 * @param p_key
	 *            the key
	 * @param p_defaultValue
	 *            the default value
	 * @return the argument
	 */
	public int getArgument(final String p_key, final int p_defaultValue) {
		int ret = p_defaultValue;

		if (m_arguments.containsKey(p_key)) {
			try {
				ret = Integer.parseInt(m_arguments.getProperty(p_key));
			} catch (final Exception e) {}
		}

		return ret;
	}

	/**
	 * Checks if an argument for the given key exists
	 * @param p_key
	 *            the key
	 * @return true if an argument exists, false otherwise
	 */
	public boolean containsArgument(final String p_key) {
		return m_arguments.containsKey(p_key);
	}

}
