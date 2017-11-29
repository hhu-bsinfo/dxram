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

package de.hhu.bsinfo.dxgraph.data;

import java.util.Arrays;

import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

/**
 * Extending the basic vertex class with dynamic properties.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
public class VertexWithProperties extends Vertex {

    private Property[] m_properties = new Property[0];

    /**
     * Constructor
     */
    public VertexWithProperties() {
    }

    /**
     * Constructor
     *
     * @param p_id
     *         Chunk id to assign.
     */
    public VertexWithProperties(final long p_id) {
        super(p_id);
    }

    // -----------------------------------------------------------------------------

    /**
     * Add a new property to the currently existing list.
     * This will expand the array by one entry and
     * add the new property at the end.
     *
     * @param p_property
     *         Property to add.
     */
    public void addProperty(final Property p_property) {
        setPropertyCount(m_properties.length + 1);
        m_properties[m_properties.length - 1] = p_property;
    }

    /**
     * Get the property array.
     *
     * @return Property array.
     */
    public Property[] getProperties() {
        return m_properties;
    }

    /**
     * Get the number of properties of this vertex.
     *
     * @return Number of properties.
     */
    public int getPropertyCount() {
        return m_properties.length;
    }

    /**
     * Resize the property array.
     *
     * @param p_count
     *         Number of properties to resize to.
     */
    public void setPropertyCount(final int p_count) {
        if (p_count != m_properties.length) {
            // grow or shrink array
            m_properties = Arrays.copyOf(m_properties, p_count);
        }
    }

    // -----------------------------------------------------------------------------

    @Override
    public void importObject(final Importer p_importer) {
        super.importObject(p_importer);

        m_properties = new Property[p_importer.readInt(m_properties == null ? 0 : m_properties.length)];
        for (int i = 0; i < m_properties.length; i++) {
            m_properties[i] = PropertyManager.createInstance(p_importer.readShort(m_properties[i] == null ? 0 : m_properties[i].getID()));
            p_importer.importObject(m_properties[i]);
        }
    }

    @Override
    public int sizeofObject() {
        int size = super.sizeofObject();

        for (Property property : m_properties) {
            size += Short.BYTES + property.sizeofObject();
        }

        return size;
    }

    @Override
    public void exportObject(final Exporter p_exporter) {
        super.exportObject(p_exporter);

        p_exporter.writeInt(m_properties.length);
        for (Property property : m_properties) {
            p_exporter.writeShort(property.getID());
            p_exporter.exportObject(property);
        }
    }

    @Override
    public String toString() {
        return "VertexWithProperties[m_id " + Long.toHexString(getID()) + ", m_neighborsAreEdgeObjects " + areNeighborsEdgeObjects() + ", m_locked " +
                isLocked() + ", m_propertiesCount " + m_properties.length + ", m_neighborsCount " + getNeighborCount() + "]: ";
    }
}
