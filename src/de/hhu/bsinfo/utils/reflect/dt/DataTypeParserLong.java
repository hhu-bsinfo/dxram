package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserLong implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "long";
	}

	@Override
	public Object parse(java.lang.String p_str) 
	{
		if (p_str.length() > 1)
		{
			String tmp = p_str.substring(0, 2);
			if (tmp.equals("0x"))
				return java.lang.Long.parseLong(p_str.substring(2), 16);
			else if (tmp.equals("0b"))
				return java.lang.Long.parseLong(p_str.substring(2), 2);
			else if (tmp.equals("0o"))
				return java.lang.Long.parseLong(p_str.substring(2), 8);
		}
		
		return java.lang.Long.parseLong(p_str);
	}
}
