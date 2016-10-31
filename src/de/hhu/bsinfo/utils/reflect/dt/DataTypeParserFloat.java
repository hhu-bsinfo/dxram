package de.hhu.bsinfo.utils.reflect.dt;

/**
 * Implementation of a float parser.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DataTypeParserFloat implements DataTypeParser {
    @Override
    public java.lang.String getTypeIdentifer() {
        return "float";
    }

    @Override
    public Class<?> getClassToConvertTo() {
        return Float.class;
    }

    @Override
    public Object parse(final java.lang.String p_str) {
        try {
            return java.lang.Float.parseFloat(p_str);
        } catch (final NumberFormatException e) {
            return new java.lang.Float(0.0f);
        }
    }
}
