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

package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Base class for a property to be attached to a Vertex or an Edge.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
public abstract class Property<T extends Property> implements Importable, Exportable {

    private static short PROPERY_ID = -1;

    private short m_propertyId = PROPERY_ID;

    /**
     * Constructor
     */
    public Property() {
        assert m_propertyId != -1;
    }

    /**
     * Get the property type id.
     * @return Property type id.
     */
    public short getID() {
        return m_propertyId;
    }
}
