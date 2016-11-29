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

package de.hhu.bsinfo.dxram.data;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Interface for any kind of data structure that can be stored and read from
 * memory. Implement this with any object you want to put/get from the memory system.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 11.12.2015
 */
public interface DataStructure extends Importable, Exportable {
    /**
     * Get the unique identifier of this data structure.
     *
     * @return Unique identifier.
     */
    long getID();

    /**
     * Set the unique identifier of this data structure.
     *
     * @param p_id
     *     ID to set.
     */
    void setID(long p_id);
}
