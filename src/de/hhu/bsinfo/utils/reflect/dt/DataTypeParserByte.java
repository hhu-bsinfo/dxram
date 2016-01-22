package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserByte implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "byte";
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return java.lang.Byte.parseByte(p_str);
	}
}
