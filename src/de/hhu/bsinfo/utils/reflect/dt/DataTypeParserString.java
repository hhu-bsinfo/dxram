
package de.hhu.bsinfo.utils.reflect.dt;

/**
 * Implementation of a string parser.
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 26.01.16
 */
public class DataTypeParserString implements DataTypeParser {
	@Override
	public java.lang.String getTypeIdentifer() {
		return "str";
	}

	@Override
	public Class<?> getClassToConvertTo() {
		return String.class;
	}

	@Override
	public Object parse(final java.lang.String p_str) {
		return new java.lang.String(p_str);
	}
}
