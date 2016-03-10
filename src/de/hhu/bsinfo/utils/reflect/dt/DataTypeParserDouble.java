package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserDouble implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "double";
	}
	
	@Override
	public Class<?> getClassToConvertTo() {
		return Double.class;
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return java.lang.Double.parseDouble(p_str);
	}
}
