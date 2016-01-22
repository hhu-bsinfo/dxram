package de.hhu.bsinfo.utils.reflect.dt;

public class DataTypeParserInt implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "int";
	}

	@Override
	public Object parse(java.lang.String p_str) {
		return java.lang.Integer.parseInt(p_str);
	}
}
