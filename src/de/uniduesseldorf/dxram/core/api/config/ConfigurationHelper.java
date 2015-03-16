
package de.uniduesseldorf.dxram.core.api.config;

import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationEntry;

/**
 * Simplifies the access to the DXRAM configuration
 * @author Florian Klein
 *         03.09.2013
 */
public final class ConfigurationHelper {

	// Attributes
	private Configuration m_configuration;

	// Constructors
	/**
	 * Creates an instance of ConfigurationHelper
	 * @param p_configuration
	 *            the configuration to use
	 */
	public ConfigurationHelper(final Configuration p_configuration) {
		m_configuration = p_configuration;
	}

	// Getters
	/**
	 * Gets the configuration
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return m_configuration;
	}

	// Methods
	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public String getStringValue(final ConfigurationEntry<String> p_entry) {
		String ret;

		ret = m_configuration.getValue(p_entry.getKey());
		if (ret == null) {
			ret = p_entry.getDefaultValue();
		}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public byte getByteValue(final ConfigurationEntry<Byte> p_entry) {
		byte ret = p_entry.getDefaultValue();

		try {
			ret = Byte.parseByte(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public short getShortValue(final ConfigurationEntry<Short> p_entry) {
		short ret = p_entry.getDefaultValue();

		try {
			ret = Short.parseShort(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public int getIntValue(final ConfigurationEntry<Integer> p_entry) {
		int ret = p_entry.getDefaultValue();

		try {
			ret = Integer.parseInt(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public long getLongValue(final ConfigurationEntry<Long> p_entry) {
		long ret = p_entry.getDefaultValue();

		try {
			ret = Long.parseLong(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public float getFloatValue(final ConfigurationEntry<Float> p_entry) {
		float ret = p_entry.getDefaultValue();

		try {
			ret = Float.parseFloat(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public double getDoubleValue(final ConfigurationEntry<Double> p_entry) {
		double ret = p_entry.getDefaultValue();

		try {
			ret = Double.parseDouble(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public boolean getBooleanValue(final ConfigurationEntry<Boolean> p_entry) {
		boolean ret = p_entry.getDefaultValue();

		try {
			ret = Boolean.parseBoolean(m_configuration.getValue(p_entry.getKey()));
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public char getCharValue(final ConfigurationEntry<Character> p_entry) {
		char ret = p_entry.getDefaultValue();

		try {
			ret = m_configuration.getValue(p_entry.getKey()).charAt(0);
		} catch (final RuntimeException e) {}

		return ret;
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setStringValue(final ConfigurationEntry<String> p_entry, final String p_value) {
		m_configuration.setValue(p_entry.getKey(), p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setByteValue(final ConfigurationEntry<Byte> p_entry, final byte p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setShortValue(final ConfigurationEntry<Short> p_entry, final short p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setIntValue(final ConfigurationEntry<Integer> p_entry, final int p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setLongValue(final ConfigurationEntry<Long> p_entry, final long p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setFloatValue(final ConfigurationEntry<Float> p_entry, final float p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setDoubleValue(final ConfigurationEntry<Double> p_entry, final double p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setBooleanValue(final ConfigurationEntry<Boolean> p_entry, final boolean p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setCharValue(final ConfigurationEntry<Character> p_entry, final char p_value) {
		m_configuration.setValue(p_entry.getKey(), "" + p_value);
	}

}
