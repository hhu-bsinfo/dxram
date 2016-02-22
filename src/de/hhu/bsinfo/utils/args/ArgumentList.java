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
		return (T) m_arguments.get(p_key).getValue();
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
	}
	
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
	
	public static class Argument
	{
		private String m_key = null;
		private Object m_value = null;
		private boolean m_isOptional = false;
		private String m_description = new String();
		
		public Argument(final String p_key, final Object p_value)
		{
			m_key = p_key;
			m_value = p_value;
		}
		
		public Argument(final String p_key, final Object p_value, final boolean p_isOptional, final String p_description)
		{
			m_key = p_key;
			m_value = p_value;
			m_isOptional = p_isOptional;
			m_description = p_description;
		}
		
		public String getKey()
		{
			return m_key;
		}
		
		public Object getValue()
		{
			return m_value;
		}
		
		public <T> T getValue(final Class<T> p_class)
		{
			if (!p_class.isInstance(m_value))
			{
				assert 1 == 2;
				return null;
			}
			
			return p_class.cast(m_value);
		}
		
		public boolean isOptional()
		{
			return m_isOptional;
		}
		
		public String getDescription()
		{
			return m_description;
		}
		
		public boolean isAvailable()
		{
			return m_value != null;
		}
		
		public String toString()
		{
			return m_key + "[m_isOptional " + m_isOptional + ", m_description " + m_description + "]: " + m_value;
		}
	}
}
