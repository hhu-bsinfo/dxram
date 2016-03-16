package de.hhu.bsinfo.utils.reflect.dt;

/**
 * Implementation of a bool parser.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class DataTypeParserBool implements DataTypeParser
{
	@Override
	public java.lang.String getTypeIdentifer() {
		return "bool";
	}

	@Override
	public Class<?> getClassToConvertTo() {
		return Boolean.class;
	}
	
	@Override
	public Object parse(java.lang.String p_str) {
		// allow 0 and 1 as well
		if (p_str.equals("0")) {
			return new java.lang.Boolean(false);
		} else if (p_str.equals("1")) {
			return new java.lang.Boolean(true);
		} else {
			return java.lang.Boolean.parseBoolean(p_str);
		}
	}
}
