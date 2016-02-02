package de.hhu.bsinfo.utils.main;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import de.hhu.bsinfo.utils.Pair;

public class MainArguments {
	private Map<String, Object> m_arguments = new HashMap<String, Object>();
	
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
	
	@SuppressWarnings("unchecked")
	public <T> T getArgument(final Pair<String, T> p_default) {
		T val = (T) getArgument(p_default.first(), p_default.second().getClass());
		if (val == null)
			val = p_default.second();
		return val;
	}
	
	public <T> void setArgument(final Pair<String, T> p_default)
	{
		setArgument(p_default.first(), p_default.second());
	}
	
	public <T> void setArgument(final String p_key, final T p_value)
	{
		m_arguments.put(p_key, p_value);
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
