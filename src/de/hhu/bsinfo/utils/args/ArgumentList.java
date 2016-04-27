
package de.hhu.bsinfo.utils.args;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.utils.reflect.dt.DataTypeParser;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserBool;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserByte;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserDouble;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserFloat;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserInt;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserLong;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserShort;
import de.hhu.bsinfo.utils.reflect.dt.DataTypeParserString;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverter;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverterGBToByte;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverterKBToByte;
import de.hhu.bsinfo.utils.reflect.unit.UnitConverterMBToByte;

/**
 * Easier to handle argument list/map within an application.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public class ArgumentList {
	private Map<String, Argument> m_arguments = new HashMap<String, Argument>();

	/**
	 * Get an argument specified by the provided key from the list.
	 * @param p_key
	 *            Key for the argument to get.
	 * @return Argument matching the key or null if not available.
	 */
	public Argument getArgument(final String p_key) {

		return m_arguments.get(p_key);
	}

	/**
	 * Get a value from the argument list and convert it to the specified data type.
	 * @param <T>
	 *            Type of the argument value
	 * @param p_key
	 *            Key to get the value of.
	 * @param p_class
	 *            Class to convert the value to.
	 * @return Converted object of the value the key was associated with or null if missing or converting failed.
	 */
	public <T> T getArgumentValue(final String p_key, final Class<T> p_class) {
		Argument arg = m_arguments.get(p_key);
		if (arg == null) {
			return null;
		}

		return arg.getValue(p_class);
	}

	/**
	 * Get the size of the argument list.
	 * @return Number of arguments on the list.
	 */
	public int getSize() {
		return m_arguments.size();
	}

	/**
	 * Get an argument from the list, but return the provided default
	 * argument if not available.
	 * @param p_default
	 *            Default argument to return if argument not available.
	 * @return Argument specified by key of the default argument.
	 */
	public Argument getArgument(final Argument p_default) {
		Argument arg = getArgument(p_default.getKey());
		if (arg == null) {
			arg = p_default;
		}

		return arg;
	}

	/**
	 * Get a value from the argument list and convert it to the specified data type.
	 * @param <T>
	 *            Type of the argument value
	 * @param p_default
	 *            Argument taking the key from.
	 * @param p_class
	 *            Class to convert the value to.
	 * @return Converted object of the value the key was associated with or null if missing or converting failed.
	 */
	public <T> T getArgumentValue(final Argument p_default, final Class<T> p_class) {
		return getArgument(p_default).getValue(p_class);
	}

	/**
	 * Set the value of an argument.
	 * If the argument does not exist, it will be created and added.
	 * @param p_key
	 *            Key of the argument.
	 * @param p_value
	 *            Value for the argument.
	 * @param p_unit
	 *            Unit to this value is stored as. Used for converting.
	 */
	public void setArgument(final String p_key, final String p_value, final String p_unit) {
		Argument arg = m_arguments.get(p_key);
		if (arg != null) {
			arg = new Argument(arg.getKey(), p_value, p_unit, arg.isOptional(), arg.getDescription());
		} else {
			arg = new Argument(p_key, p_value, p_unit, false, "");
		}

		m_arguments.put(p_key, arg);
	}

	/**
	 * Add/Override an argument's value.
	 * @param p_argument
	 *            Argument to add (takes the key).
	 * @param p_value
	 *            New value.
	 */
	public void setArgument(final Argument p_argument, final Object p_value) {
		Argument arg = new Argument(p_argument);
		arg.m_value = p_value.toString();
		m_arguments.put(arg.getKey(), arg);
	}

	/**
	 * Add/Override an argument.
	 * @param p_argument
	 *            Argument to add.
	 */
	public void setArgument(final Argument p_argument) {
		m_arguments.put(p_argument.getKey(), p_argument);
	}

	/**
	 * Clear the argument list.
	 */
	public void clear() {
		m_arguments.clear();
	}

	/**
	 * Get the size of the argument list.
	 * @return Number of arguments.
	 */
	public int size() {
		return m_arguments.size();
	}

	/**
	 * Verifies if all values are non null for non optional arguments.
	 * @return True if all non optional arguments have non null values, false otherwise.
	 */
	public boolean checkArguments() {
		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			if (!entry.getValue().isAvailable() && !entry.getValue().isOptional()) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Creates a usage description to printed to the console.
	 * @param p_applicationName
	 *            Name of the application.
	 * @return Usage string with arguments and description.
	 */
	public String createUsageDescription(final String p_applicationName) {
		String str = new String();

		str += "Usage: " + p_applicationName;
		// have non optional arguments first
		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			Argument arg = entry.getValue();

			if (!arg.isOptional()) {
				str += " <" + arg.getKey() + ":value>";
			}
		}

		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			Argument arg = entry.getValue();

			if (arg.isOptional()) {
				str += " [" + arg.getKey() + ":value]";
			}
		}

		// also add descriptions
		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			Argument arg = entry.getValue();

			if (!arg.isOptional()) {
				str += "\n\t" + arg.getKey() + ": " + arg.getDescription();
			}
		}

		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			Argument arg = entry.getValue();

			if (arg.isOptional()) {
				str += "\n\t" + arg.getKey() + ": " + arg.getDescription();
			}
		}

		return str;
	}

	@Override
	public String toString() {
		String str = new String();

		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			str += entry.getValue() + "\n";
		}

		return str;
	}

	/**
	 * Get the backend argument map. There are situations when we have to iterate
	 * the argument list. Otherwise don't use this call.
	 * @return Map with arguments.
	 */
	public Map<String, Argument> getArgumentMap() {
		return m_arguments;
	}

	/**
	 * A single argument of a argument list.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.02.16
	 */
	public static class Argument {
		private String m_key;
		private String m_value;
		private String m_convert = new String();
		private boolean m_isOptional;
		private String m_description = new String();

		private static Map<Class<?>, DataTypeParser> ms_dataTypeParsers = new HashMap<Class<?>, DataTypeParser>();
		private static Map<String, UnitConverter> ms_unitConverters = new HashMap<String, UnitConverter>();
		static {
			ms_dataTypeParsers.put(String.class, new DataTypeParserString());
			ms_dataTypeParsers.put(Byte.class, new DataTypeParserByte());
			ms_dataTypeParsers.put(Short.class, new DataTypeParserShort());
			ms_dataTypeParsers.put(Integer.class, new DataTypeParserInt());
			ms_dataTypeParsers.put(Long.class, new DataTypeParserLong());
			ms_dataTypeParsers.put(Float.class, new DataTypeParserFloat());
			ms_dataTypeParsers.put(Double.class, new DataTypeParserDouble());
			ms_dataTypeParsers.put(Boolean.class, new DataTypeParserBool());

			// add default unit converters
			addUnitConverter(new UnitConverterKBToByte());
			addUnitConverter(new UnitConverterMBToByte());
			addUnitConverter(new UnitConverterGBToByte());
		}

		/**
		 * Constructor
		 * @param p_key
		 *            Key identifying the argument (must be unique).
		 * @param p_value
		 *            Value of the argument
		 */
		public Argument(final String p_key, final String p_value) {
			m_key = p_key;
			m_value = p_value;
			m_isOptional = false;
		}

		/**
		 * Constructor
		 * @param p_key
		 *            Key identifying the argument (must be unique).
		 * @param p_value
		 *            Value of the argument
		 * @param p_convert
		 *            String to tell if the value needs conversion.
		 */
		public Argument(final String p_key, final String p_value, final String p_convert) {
			m_key = p_key;
			m_value = p_value;
			m_isOptional = false;
			m_convert = p_convert;
		}

		/**
		 * Constructor
		 * @param p_key
		 *            Key identifiying the argument (must be unique).
		 * @param p_value
		 *            Value of the argument.
		 * @param p_isOptional
		 *            True if the argument is optional, i.e. is allowed to be null, false otherwise.
		 * @param p_description
		 *            Description for the argument (used when creating usage string).
		 */
		public Argument(final String p_key, final String p_value, final boolean p_isOptional,
				final String p_description) {
			m_key = p_key;
			m_value = p_value;
			m_isOptional = p_isOptional;
			m_description = p_description;
		}

		/**
		 * Constructor
		 * @param p_key
		 *            Key identifiying the argument (must be unique).
		 * @param p_value
		 *            Value of the argument.
		 * @param p_convert
		 *            String to tell if the value needs conversion.
		 * @param p_isOptional
		 *            True if the argument is optional, i.e. is allowed to be null, false otherwise.
		 * @param p_description
		 *            Description for the argument (used when creating usage string).
		 */
		public Argument(final String p_key, final String p_value, final String p_convert, final boolean p_isOptional,
				final String p_description) {
			m_key = p_key;
			m_value = p_value;
			m_convert = p_convert;
			m_isOptional = p_isOptional;
			m_description = p_description;
		}

		/**
		 * Copy constructor
		 * @param p_argument
		 *            Argument to copy
		 */
		public Argument(final Argument p_argument) {
			m_key = p_argument.m_key;
			m_value = p_argument.m_value;
			m_convert = p_argument.m_convert;
			m_isOptional = p_argument.m_isOptional;
			m_description = p_argument.m_description;
		}

		/**
		 * Add a data type converter to allow converting of values to different types.
		 * @param p_converter
		 *            Data type converter converter to add.
		 */
		public static void addDataTypeConverter(final DataTypeParser p_converter) {
			ms_dataTypeParsers.put(p_converter.getClassToConvertTo(), p_converter);
		}

		/**
		 * Add a unit converter to allow unit conversion of arguments.
		 * @param p_converter
		 *            Unit converter to add.
		 */
		public static void addUnitConverter(final UnitConverter p_converter) {
			ms_unitConverters.put(p_converter.getUnitIdentifier(), p_converter);
		}

		/**
		 * Get the key.
		 * @return Argument key.
		 */
		public String getKey() {
			return m_key;
		}

		/**
		 * Get the arguments value converter.
		 * @param <T>
		 *            Type of the class
		 * @param p_class
		 *            Type of the value to cast to.
		 * @return Value.
		 */
		public <T> T getValue(final Class<T> p_class) {
			if (m_value == null) {
				return null;
			}

			DataTypeParser parser = ms_dataTypeParsers.get(p_class);
			Object val = parser.parse(m_value);

			if (!m_convert.isEmpty()) {
				UnitConverter converter = ms_unitConverters.get(m_convert);
				val = converter.convert(val);
			}

			if (!p_class.isInstance(val)) {
				assert 1 == 2;
				return null;
			}

			return p_class.cast(val);
		}

		/**
		 * Get the unconverted value as string.
		 * @return Value.
		 */
		public String getValue() {
			return m_value;
		}

		/**
		 * Is the argument optional.
		 * @return True for optional.
		 */
		public boolean isOptional() {
			return m_isOptional;
		}

		/**
		 * Get the description of the argument.
		 * @return Description string.
		 */
		public String getDescription() {
			return m_description;
		}

		/**
		 * Check if the value of the argument is available, i.e. non null.
		 * @return True if available, false otherwise.
		 */
		public boolean isAvailable() {
			return m_value != null;
		}

		@Override
		public String toString() {
			return m_key + "[m_isOptional " + m_isOptional + ", m_description " + m_description + "]: " + m_value;
		}
	}
}
