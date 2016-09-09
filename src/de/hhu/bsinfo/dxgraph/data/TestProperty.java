package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Property implementation for testing.
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 09.09.16
 */
public class TestProperty extends Property<TestProperty> {

	static {
		PropertyManager.registerPropertyClass(TestProperty.class);
	}

	private int m_value;

	/**
	 * Constructor
	 */
	public TestProperty() {

	}

	/**
	 * Get the value.
	 *
	 * @return Value.
	 */
	public int getValue() {
		return m_value;
	}

	/**
	 * Set the value.
	 *
	 * @param val Value.
	 */
	public void setValue(final int val) {
		m_value = val;
	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		p_exporter.writeInt(m_value);
	}

	@Override
	public void importObject(final Importer p_importer) {
		m_value = p_importer.readInt();
	}

	@Override
	public int sizeofObject() {
		return Integer.BYTES;
	}
}
