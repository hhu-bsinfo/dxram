/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
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
