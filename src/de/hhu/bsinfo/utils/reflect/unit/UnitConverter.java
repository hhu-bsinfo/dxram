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

package de.hhu.bsinfo.utils.reflect.unit;

/**
 * Base class for a conversion interface, that handles converting
 * some input value to some output. Typical usage: megabyte to byte
 * on an integer/long value.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public interface UnitConverter {
    /**
     * Get the string identifier for the unit we are targeting.
     *
     * @return String identifier of targeting data type.
     */
    String getUnitIdentifier();

    /**
     * Convert the specified value.
     *
     * @param p_value
     *         Value to convert.
     * @return Ouput of the conversion or null if failed.
     */
    Object convert(Object p_value);
}
