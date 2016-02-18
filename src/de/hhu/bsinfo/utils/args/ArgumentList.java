package de.hhu.bsinfo.utils.args;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.utils.Pair;

/**
 * Easier to handle argument list/map within an application.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 *
 */
public class ArgumentList {
	private Map<String, Object> m_arguments = new HashMap<String, Object>();
	
	/**
	 * Get an argument from the list.
	 * @param p_key Name of the argument.
	 * @param p_type Type expected of that argument.
	 * @return Argument value or null if argument with specified name does not exist.
	 */
	public <T> T getArgument(final String p_key, final Class<T> p_type) {
		
		Object val = m_arguments.get(p_key);
		if (val == null)
			return null;
		if (!p_type.isInstance(val))
		{
			assert 1 == 2;
			return null;
		}
		
		return p_type.cast(val);
	}
	
	/**
	 * Get an argument from the list.
	 * @param p_default Pair of argument name and default value.
	 * @return If argument exists, returns the value of the argument otherwise the provided default value.
	 */
	@SuppressWarnings("unchecked")
	public <T> T getArgument(final Pair<String, T> p_default) {
		T val = (T) getArgument(p_default.first(), p_default.second().getClass());
		if (val == null)
			val = p_default.second();
		return val;
	}
	
	/**
	 * Set an argument value.
	 * @param p_default Pair of name of the argument and default value to set.
	 */
	public <T> void setArgument(final Pair<String, T> p_default)
	{
		m_arguments.put(p_default.first(), p_default.second());
	}
	
	/**
	 * Set an argument value.
	 * @param p_key Name of the argument.
	 * @param p_value Value of the argument.
	 */
	public <T> void setArgument(final Pair<String, T> p_key, final T p_value)
	{
		m_arguments.put(p_key.first(), p_value);
	}
	
	/**
	 * Set an argument/default value.
	 * @param p_key Name of the argument.
	 * @param p_value Value of the argument.
	 */
	public <T> void setArgument(final String p_key, final T p_value)
	{
		m_arguments.put(p_key, p_value);
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
	
	@Override
	public String toString()
	{
		String str = new String();
		
		for (Entry<String, Object> entry : m_arguments.entrySet()) {
			str += entry.getKey() + ":" + entry.getValue() + "\n";
		}
		
		return str;
	}
}
