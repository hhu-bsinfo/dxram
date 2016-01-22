package de.hhu.bsinfo.utils.reflect.dt;

public interface DataTypeParser
{
	public String getTypeIdentifer();
	
	public Object parse(final String p_str);
}
