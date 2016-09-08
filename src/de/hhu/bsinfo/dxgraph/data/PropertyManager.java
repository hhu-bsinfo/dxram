package de.hhu.bsinfo.dxgraph.data;

import java.util.ArrayList;

/**
 * Created by nothaas on 9/8/16.
 */
public class PropertyManager {

	private static ArrayList<Class<? extends Property>> m_registeredProperties = new ArrayList<>(10);

	/**
	 * Static class.
	 */
	private PropertyManager() {

	}

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
