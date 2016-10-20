package de.hhu.bsinfo.dxgraph.data;

import java.util.ArrayList;

/**
 * Manager class for property classes created. Make sure to register
 * any newly created property classes here.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 09.09.16
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
	 *
	 * @param p_propertyId Registered id of a property object to create.
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
	 *
	 * @param p_clazz Property class to register.
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
