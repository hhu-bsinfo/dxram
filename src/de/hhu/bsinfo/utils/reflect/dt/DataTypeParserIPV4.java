/*
 * Copyright (C) 2016 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

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
