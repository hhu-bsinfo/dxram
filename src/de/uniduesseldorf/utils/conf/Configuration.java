package de.uniduesseldorf.utils.conf;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Configuration 
{
	public static final String KEY_SEQ_SEPARATOR = "/";
	
	private String m_name = null;
	Map<String, Map<Integer, Object>> m_parameters = new HashMap<String, Map<Integer, Object>>();
	
	public Configuration(final String p_name) 
	{
		m_name = p_name;
	}
	
	public String getName() {
		return "Configuration " + m_name;
	}
	
	@Override
	public String toString()
	{
		String str = new String();
		
		str += getName() + "\n";
		
		for (Entry<String, Map<Integer, Object>> it : m_parameters.entrySet())
		{
			str += it.getKey() + "\n";
			for (Entry<Integer, Object> it2 : it.getValue().entrySet())
			{
				str += "\t" + it2.getKey() + " | " + it2.getValue().getClass().getName() + " | " + it2.getValue() + "\n";
			}
		}
		
		return str;
	}
	
	public <T> T GetValue(final String p_key, final Class<T> p_type)
	{
		return GetValue(p_key, 0, p_type);
	}
	
	public <T> T GetValue(final String p_key, final int p_index, final Class<T> p_type)
	{
		String key = genAndNormalizeKey(p_key);
		
		Map<Integer, Object> map = m_parameters.get(key);
		if (map == null)
			return null;
		
		Object val = map.get(p_index);
		if (val == null)
			return null;
		
		// check datatype
		if (!p_type.isInstance(val))
		{
			assert 1 == 2;
			return null;
		}
		
		return p_type.cast(val);
	}
	
	public <T> Map<Integer, T> GetValues(final String p_key, final Class<T> p_type)
	{
		String key = genAndNormalizeKey(p_key);
		
		Map<Integer, Object> map = m_parameters.get(key);
		if (map == null)
			return null;
		
		Map<Integer, T> retMap = new HashMap<Integer, T>();
		for (Entry<Integer, Object> entry : map.entrySet())
		{
			// check datatype
			if (!p_type.isInstance(entry.getValue()))
			{
				assert 1 == 2;
				return null;
			}
			
			retMap.put(entry.getKey(), p_type.cast(entry.getValue()));
		}
		
		return retMap;
	}
	
	public <T> boolean AddValue(final String p_key, final T p_value) 
	{
		return AddValue(p_key, p_value, true);
	}
	
	public <T> boolean AddValue(final String p_key, final T p_value, final boolean p_replaceExisting) 
	{
		return AddValue(p_key, 0, p_value, p_replaceExisting);
	}
	
	public <T> boolean AddValue(final String p_key, final int p_index, final T p_value)
	{
		return AddValue(p_key, p_index, p_value, true);
	}
	
	public <T> boolean AddValue(final String p_key, final int p_index, final T p_value, final boolean p_replaceExisting)
	{
		String key = genAndNormalizeKey(p_key);
		
		Map<Integer, Object> map = m_parameters.get(key);
		if (map == null)
		{
			// don't have a sub map, yet
			// generate
			map = new HashMap<Integer, Object>();
			m_parameters.put(key, map);
			
			// add value to sub map
			map.put(p_index, p_value);
			return true;
		}
		
		// sub map already exists, i.e. has entries
		Object val = map.get(p_index);
		if (val == null)
		{
			// value does not exist, insert
			map.put(p_index, p_value);
			return true;
		}
		else
		{
			// value exists
			if (p_replaceExisting)
			{
				map.put(p_index, p_value);
				return true;
			}
			else
			{
				return false;
			}
		}
	}
	
	private String genAndNormalizeKey(final String p_key)
	{
		String ret = p_key;
		
		if (!p_key.startsWith(KEY_SEQ_SEPARATOR))
			ret = KEY_SEQ_SEPARATOR + ret;
		if (p_key.endsWith(KEY_SEQ_SEPARATOR))
			ret = ret.substring(0, ret.length() - 1);
		
		return ret;
	}
}
