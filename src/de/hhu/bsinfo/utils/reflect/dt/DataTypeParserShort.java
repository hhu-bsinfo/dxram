package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserShort implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "short";
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return java.lang.Short.parseShort(p_str);
	}
}