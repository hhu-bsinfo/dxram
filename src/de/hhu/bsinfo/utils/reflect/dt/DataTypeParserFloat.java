package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserFloat implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "float";
	}
	
	@Override
	public Class<?> getClassToConvertTo() {
		return Float.class;
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return java.lang.Float.parseFloat(p_str);
	}
}
