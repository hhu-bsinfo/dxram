package de.hhu.bsinfo.utils.reflect.dt;

/**
 * Implementation of a short parser.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class DataTypeParserShort implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "short";
	}
	
	@Override
	public Class<?> getClassToConvertTo() {
		return Short.class;
	}

	@Override
	public Object parse(java.lang.String p_str) 
	{
		if (p_str.length() > 1)
		{
			String tmp = p_str.substring(0, 2);
			if (tmp.equals("0x"))
				return (short) Integer.parseInt(p_str.substring(2), 16);
			else if (tmp.equals("0b"))
				return (short) Integer.parseInt(p_str.substring(2), 2);
			else if (tmp.equals("0o"))
				return (short) Integer.parseInt(p_str.substring(2), 8);
		}
		
		return java.lang.Short.parseShort(p_str);
	}
}