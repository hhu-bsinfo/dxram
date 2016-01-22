package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserLong implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "long";
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return java.lang.Long.parseLong(p_str);
	}
}
