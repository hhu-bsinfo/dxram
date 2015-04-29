
package de.uniduesseldorf.dxram.test;

import java.util.Properties;

public class ArgumentHelper {

	// Attributes
	private Properties m_arguments;

	// Constructors
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
	public String getArgument(final String p_key) {
		return m_arguments.getProperty(p_key);
	}

	public int getArgument(final String p_key, final int p_defaultValue) {
		int ret = p_defaultValue;

		if (m_arguments.containsKey(p_key)) {
			try {
				ret = Integer.parseInt(m_arguments.getProperty(p_key));
			} catch (final Exception e) {}
		}

		return ret;
	}

	public boolean containsArgument(final String p_key) {
		return m_arguments.containsKey(p_key);
	}

}
