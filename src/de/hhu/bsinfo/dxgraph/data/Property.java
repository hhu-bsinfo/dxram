package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Importable;

/**
 * Created by nothaas on 9/8/16.
 */
public abstract class Property<T extends Property> implements Importable, Exportable {

	private static short PROPERY_ID = -1;

	private short m_propertyId = PROPERY_ID;

	public Property() {
		assert m_propertyId != -1;
	}

	/**
	 * Get the property type id.
	 *
	 * @return Property type id.
	 */
	public short getID() {
		return m_propertyId;
	}
}
