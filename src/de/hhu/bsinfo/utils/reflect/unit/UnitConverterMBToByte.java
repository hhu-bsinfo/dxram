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

package de.hhu.bsinfo.utils.reflect.unit;

/**
 * Convert a megabyte units value to byte units.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public class UnitConverterMBToByte implements UnitConverter {
    @Override
    public String getUnitIdentifier() {
        return "mb2b";
    }

    @Override
    public Object convert(final Object p_value) {
        if (p_value instanceof Byte) {
            return ((Byte) p_value) * 1024 * 1024;
        } else if (p_value instanceof Short) {
            return ((Short) p_value) * 1024 * 1024;
        } else if (p_value instanceof Integer) {
            return ((Integer) p_value) * 1024 * 1024;
        } else if (p_value instanceof Long) {
            return ((Long) p_value) * 1024 * 1024;
        } else {
            return p_value;
        }
    }
}
