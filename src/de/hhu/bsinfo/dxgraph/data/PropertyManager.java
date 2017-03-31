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

import java.util.ArrayList;

/**
 * Manager class for property classes created. Make sure to register
 * any newly created property classes here.
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 09.09.2016
 */
public class PropertyManager {

    private static ArrayList<Class<? extends Property>> m_registeredProperties = new ArrayList<>(10);

    /**
     * Static class.
     */
    private PropertyManager() {

    }

    /**
     * Create a new instance of a property object by id.
     * @param p_propertyId
     *            Registered id of a property object to create.
     * @return Property object created or null if no object with the specified id is registered.
     */
    public static Property createInstance(final short p_propertyId) {
        Class<? extends Property> clazz;
        try {
            clazz = m_registeredProperties.get(p_propertyId);
        } catch (final IndexOutOfBoundsException e) {
            return null;
        }

        try {
            return clazz.newInstance();
        } catch (final Exception e) {
            return null;
        }
    }

    /**
     * Register a property class.
     * @param p_clazz
     *            Property class to register.
     */
    public static synchronized void registerPropertyClass(final Class<? extends Property> p_clazz) {
        if (m_registeredProperties.size() == Short.MAX_VALUE) {
            throw new RuntimeException("Max number of properties to register exceeded");
        }

        try {
            p_clazz.getField("PROPERTY_ID").setShort(null, (short) m_registeredProperties.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        m_registeredProperties.add(p_clazz);
    }
}
