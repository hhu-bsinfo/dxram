package de.hhu.bsinfo.utils.reflect.dt;

import java.net.InetSocketAddress;

/**
 * Implementation of an IPV4 address parser.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class DataTypeParserIPV4 implements DataTypeParser {
    @Override
    public java.lang.String getTypeIdentifer() {
        return "inetv4";
    }

    @Override
    public Class<?> getClassToConvertTo() {
        return InetSocketAddress.class;
    }

    @Override
    public Object parse(final java.lang.String p_str) {
        try {
            String[] items = p_str.split(":");
            if (items.length == 2) {
                return new InetSocketAddress(items[0], Integer.parseInt(items[1]));
            } else {
                return null;
            }
        } catch (final NumberFormatException e) {
            return new InetSocketAddress(0);
        }
    }
}
