package de.hhu.bsinfo.utils.args;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Easier to handle argument list/map within an application.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class ArgumentList {
	private Map<String, Argument> m_arguments = new HashMap<String, Argument>();
	
	/**
	 * Get an argument specified by the provided key from the list.
	 * @param p_key Key for the argument to get.
	 * @return Argument matching the key or null if not available.
	 */
	public Argument getArgument(final String p_key) {
		
		return m_arguments.get(p_key);
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getArgumentValue(final String p_key) {
		Argument arg = m_arguments.get(p_key);
		if (arg == null)
			return null;
		
		return (T) arg.getValue();
	}
	
	/**
	 * Get an argument from the list, but return the provided default
	 * argument if not available.
	 * @param p_default Default argument to return if argument not available.
	 * @return Argument specified by key of the default argument.
	 */
	public Argument getArgument(final Argument p_default) {
		Argument arg = getArgument(p_default.getKey());
		if (arg == null) {
			arg = p_default;
		}
		
		return arg;
	}
	
	@SuppressWarnings("unchecked")
	public <T> T getArgumentValue(final Argument p_default) {
		return (T) getArgument(p_default).getValue();
	}
	
	/**
	 * Add an argument to the list.
	 * This will check if a default argument is set and replace its value if available.
	 * Otherwise it creates a new argument.
	 * @param p_argument Argument to add.
	 */
	public void setArgument(final String p_key, final Object p_value)
	{
		Argument arg = m_arguments.get(p_key);
		if (arg != null)
		{
			arg.m_value = p_value;
		}
		else
			arg = new Argument(p_key, p_value, false, "");
	
		m_arguments.put(p_key, arg);
	}
	
	/**
	 * Add/Override an argument's value.
	 * @param p_argument Argument to add (takes the key).
	 * @param p_value New value.
	 */
	public void setArgument(final Argument p_argument, final Object p_value)
	{
		p_argument.m_value = p_value;
		m_arguments.put(p_argument.getKey(), p_argument);
	}	
	
	/**
	 * Add/Override an argument.
	 * @param p_argument Argument to add.
	 */
	public void setArgument(final Argument p_argument)
	{
		m_arguments.put(p_argument.getKey(), p_argument);
	}	
	
	/**
	 * Clear the argument list.
	 */
	public void clear()
	{
		m_arguments.clear();
	}
	
	/**
	 * Get the size of the argument list.
	 * @return Number of arguments.
	 */
	public int size()
	{
		return m_arguments.size();
	}
	
	/**
	 * Verifies if all values are non null for non optional arguments.
	 * @return True if all non optional arguments have non null values, false otherwise.
	 */
	public boolean checkArguments()
	{
		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			if (!entry.getValue().isAvailable()) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * Creates a usage description to printed to the console.
	 * @param p_applicationName Name of the application.
	 * @return Usage string with arguments and description.
	 */
	public String createUsageDescription(final String p_applicationName)
	{
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
				str += " [" + arg.getKey() + ":" + arg.getValue() + "]";
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
				str += "\n\t" + arg.getKey() + "[" + arg.getValue() + "]: " + arg.getDescription();
			}
		}
		
		return str;
	}
	
	@Override
	public String toString()
	{
		String str = new String();
		
		for (Entry<String, Argument> entry : m_arguments.entrySet()) {
			str += entry.getValue() + "\n";
		}
		
		return str;
	}
	
	/**
	 * A single argument of a argument list.
	 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.02.16
	 *
	 */
	public static class Argument
	{
		private String m_key = null;
		private Object m_value = null;
		private boolean m_isOptional = false;
		private String m_description = new String();
		
		/**
		 * Constructor
		 * @param p_key Key identifying the argument (must be unique).
		 * @param p_value Value of the argument
		 */
		public Argument(final String p_key, final Object p_value)
		{
			m_key = p_key;
			m_value = p_value;
		}
		
		/**
		 * Constructor
		 * @param p_key Key identifiying the argument (must be unique).
		 * @param p_value Value of the argument.
		 * @param p_isOptional True if the argument is optional, i.e. is allowed to be null, false otherwise.
		 * @param p_description Description for the argument (used when creating usage string).
		 */
		public Argument(final String p_key, final Object p_value, final boolean p_isOptional, final String p_description)
		{
			m_key = p_key;
			m_value = p_value;
			m_isOptional = p_isOptional;
			m_description = p_description;
		}
		
		/**
		 * Get the key.
		 * @return Argument key.
		 */
		public String getKey()
		{
			return m_key;
		}
		
		/**
		 * Get the argument's value.
		 * @return Value. 
		 */
		public Object getValue()
		{
			return m_value;
		}
		
		/**
		 * Get the arguments value.
		 * @param p_class Type of the value to cast to.
		 * @return Value.
		 */
		public <T> T getValue(final Class<T> p_class)
		{
			if (m_value == null)
				return null;
			
			if (!p_class.isInstance(m_value))
			{
				assert 1 == 2;
				return null;
			}
			
			return p_class.cast(m_value);
		}
		
		/**
		 * Is the argument optional.
		 * @return True for optional.
		 */
		public boolean isOptional()
		{
			return m_isOptional;
		}
		
		/**
		 * Get the description of the argument.
		 * @return Description string.
		 */
		public String getDescription()
		{
			return m_description;
		}
		
		/**
		 * Check if the value of the argument is available, i.e. non null.
		 * @return True if available, false otherwise.
		 */
		public boolean isAvailable()
		{
			return m_value != null;
		}
		
		@Override
		public String toString()
		{
			return m_key + "[m_isOptional " + m_isOptional + ", m_description " + m_description + "]: " + m_value;
		}
	}
}
