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

package de.hhu.bsinfo.utils.serialization;

/**
 * Interface to define the size of an object for importing/exporting.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.12.15
 */
public interface ObjectSize {
    /**
     * Get the size of the object.
     * This function has to return the sum in bytes of all data
     * getting exported. Make sure this value is correct and matches
     * the amount of data.
     * Also make sure to first check if the object has a dynamic
     * size. This might have influence on the value returned here
     * i.e. it can change depending on the data stored in the object.
     *
     * @return Size of the object in bytes.
     */
    int sizeofObject();
}
