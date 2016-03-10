package de.hhu.bsinfo.utils.reflect.dt;

public interface DataTypeParser
{
	public String getTypeIdentifer();
	
	public Class<?> getClassToConvertTo();
	
	public Object parse(final String p_str);
}
