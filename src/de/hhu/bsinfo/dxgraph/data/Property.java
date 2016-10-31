
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
