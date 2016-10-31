package de.hhu.bsinfo.utils.reflect.dt;

/**
 * Implementation of a double parser.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DataTypeParserDouble implements DataTypeParser {
    @Override
    public java.lang.String getTypeIdentifer() {
        return "double";
    }

    @Override
    public Class<?> getClassToConvertTo() {
        return Double.class;
    }

    @Override
    public Object parse(final java.lang.String p_str) {
        try {
            return java.lang.Double.parseDouble(p_str);
        } catch (final NumberFormatException e) {
            return new java.lang.Double(0.0);
        }
    }
}
