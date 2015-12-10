
package de.uniduesseldorf.utils.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a configuration for DXRAM
 * @author Florian Klein
 *         03.09.2013
 */
public final class Configuration {

	// Attributes
	private Map<String, String> m_entries;
	private boolean m_immutable;

	// Constructors
	/**
	 * Creates an instance of AbstractConfiguration
	 */
	public Configuration() {
		this(false);
	}

	/**
	 * Creates an instance of AbstractConfiguration
	 * @param p_immutable
	 *            defines if the configuration is immutable
	 */
	public Configuration(final boolean p_immutable) {
		m_entries = new HashMap<String, String>();
		m_immutable = p_immutable;
	}

	// Getters
	/**
	 * Defines if the configuration is immutable
	 * @return true if the configuration is immutable, false otherwise
	 */
	public synchronized boolean isImmutable() {
		return m_immutable;
	}

	// Methods
	/**
	 * Makes the configuration immutable
	 */
	public synchronized void makeImmutable() {
		m_immutable = true;
	}

	/**
	 * Gets the corresponding configuration value
	 * @param p_entry
	 *            the configuration entry
	 * @return the corresponding configuration value
	 */
	public synchronized String getValue(final ConfigurationEntry<?> p_entry) {
		return getValue(p_entry.getKey());
	}

	/**
	 * Gets the corresponding configuration value
	 * @param p_key
	 *            the key of the configuration value
	 * @return the corresponding configuration value
	 */
	public synchronized String getValue(final String p_key) {
		return m_entries.get(p_key);
	}

	/**
	 * Sets the corresponding configuration value
	 * @param p_entry
	 *            the configuration entry
	 * @param p_value
	 *            the configuration value
	 */
	public synchronized void setValue(final ConfigurationEntry<?> p_entry, final String p_value) {
		setValue(p_entry.getKey(), p_value);
	}

	/**
	 * Sets the corresponding configuration value
	 * @param p_key
	 *            the key of the configuration value
	 * @param p_value
	 *            the configuration value
	 */
	public synchronized void setValue(final String p_key, final String p_value) {
		if (!m_immutable) {
			m_entries.put(p_key, p_value);
		}
	}
	
	/**
	 * Gets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @return the value for the given ConfigurationEntry
	 */
	public String getStringValue(final ConfigurationEntry<String> p_entry) {
		String ret;

		ret = getValue(p_entry.getKey());
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
			ret = Byte.parseByte(getValue(p_entry.getKey()));
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
			ret = Short.parseShort(getValue(p_entry.getKey()));
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
		String value;

		try {
			if (p_entry.getKey().toLowerCase().endsWith("size")) {
				value = getValue(p_entry.getKey());
				if (value.toLowerCase().endsWith("kb")) {
					ret = Integer.parseInt(value.substring(0, value.length() - 2)) * 1024;
				} else if (value.toLowerCase().endsWith("mb")) {
					ret = Integer.parseInt(value.substring(0, value.length() - 2)) * 1024 * 1024;
				} else if (value.toLowerCase().endsWith("gb")) {
					ret = Integer.parseInt(value.substring(0, value.length() - 2)) * 1024 * 1024 * 1024;
				}
			} else {
				ret = Integer.parseInt(getValue(p_entry.getKey()));
			}
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
		String value;

		try {
			if (p_entry.getKey().toLowerCase().endsWith("size")) {
				value = getValue(p_entry.getKey());
				if (value.toLowerCase().endsWith("kb")) {
					ret = Long.parseLong(value.substring(0, value.length() - 2)) * 1024;
				} else if (value.toLowerCase().endsWith("mb")) {
					ret = Long.parseLong(value.substring(0, value.length() - 2)) * 1024 * 1024;
				} else if (value.toLowerCase().endsWith("gb")) {
					ret = Long.parseLong(value.substring(0, value.length() - 2)) * 1024 * 1024 * 1024;
				}
			} else {
				ret = Long.parseLong(getValue(p_entry.getKey()));
			}
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
			ret = Float.parseFloat(getValue(p_entry.getKey()));
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
			ret = Double.parseDouble(getValue(p_entry.getKey()));
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
			ret = Boolean.parseBoolean(getValue(p_entry.getKey()));
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
			ret = getValue(p_entry.getKey()).charAt(0);
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
		setValue(p_entry.getKey(), p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setByteValue(final ConfigurationEntry<Byte> p_entry, final byte p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setShortValue(final ConfigurationEntry<Short> p_entry, final short p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setIntValue(final ConfigurationEntry<Integer> p_entry, final int p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setLongValue(final ConfigurationEntry<Long> p_entry, final long p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setFloatValue(final ConfigurationEntry<Float> p_entry, final float p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setDoubleValue(final ConfigurationEntry<Double> p_entry, final double p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setBooleanValue(final ConfigurationEntry<Boolean> p_entry, final boolean p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	/**
	 * Sets the value for the given ConfigurationEntry
	 * @param p_entry
	 *            the ConfigurationEntry
	 * @param p_value
	 *            the value
	 */
	public void setCharValue(final ConfigurationEntry<Character> p_entry, final char p_value) {
		setValue(p_entry.getKey(), "" + p_value);
	}

	// Methods
	@Override
	public String toString() {
		StringBuffer ret;

		ret = new StringBuffer();

		ret.append("Configuration:\n");
		for (Entry<String, String> entry : m_entries.entrySet()) {
			ret.append(entry.getKey());
			ret.append(":\t");
			ret.append(entry.getValue());
			ret.append("\n");
		}
		ret.append("----------------------------------------");

		return ret.toString();
	}

	// Classes
	/**
	 * Describes a configuration entry
	 * @author Florian Klein
	 *         03.09.2013
	 * @param <ValueType>
	 *            the value class
	 */
	public static final class ConfigurationEntry<ValueType> {

		// Attributes
		private String m_key;
		private Class<ValueType> m_valueClass;
		private ValueType m_defaultValue;

		// Constructors
		/**
		 * Creates an instance of ConfigurationEntry
		 * @param p_key
		 *            the key of the configuration entry
		 * @param p_valueClass
		 *            the value class of the configuration entry
		 * @param p_defaultValue
		 *            the default value of the configuration entry
		 */
		public ConfigurationEntry(final String p_key, final Class<ValueType> p_valueClass, final ValueType p_defaultValue) {
			m_key = p_key;
			m_valueClass = p_valueClass;
			m_defaultValue = p_defaultValue;
		}

		// Getters
		/**
		 * Gets the key of the configuration entry
		 * @return the key
		 */
		public String getKey() {
			return m_key;
		}

		/**
		 * Gets the value class of the configuration entry
		 * @return the defaultValue
		 */
		public Class<ValueType> getValueClass() {
			return m_valueClass;
		}

		/**
		 * Gets the default value of the configuration entry
		 * @return the defaultValue
		 */
		public ValueType getDefaultValue() {
			return m_defaultValue;
		}

		// Methods
		@Override
		public String toString() {
			return "ConfigurationEntry [m_key=" + m_key + ", m_valueClass=" + m_valueClass + ", m_defaultValue=" + m_defaultValue + "]";
		}

	}
}
