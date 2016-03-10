package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserString implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "str";
	}
	
	@Override
	public Class<?> getClassToConvertTo() {
		return String.class;
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return new java.lang.String(p_str);
	}
}
