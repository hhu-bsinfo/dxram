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

package de.hhu.bsinfo.dxutils.reflect.dt;

/**
 * Base class for a parser which processes a string
 * based on a specific data format.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 26.01.2016
 */
public interface DataTypeParser {
    /**
     * Get the string identifier for the type we are targeting.
     *
     * @return String identifier of targeting data type.
     */
    String getTypeIdentifer();

    /**
     * Get the class identifier for the type we are targeting.
     *
     * @return Class identifier for the targeting data type.
     */
    Class<?> getClassToConvertTo();

    /**
     * Parse the string and create an object instance of it.
     *
     * @param p_str
     *         String to parse.
     * @return Object to be created or null if parsing failed.
     */
    Object parse(String p_str);
}
