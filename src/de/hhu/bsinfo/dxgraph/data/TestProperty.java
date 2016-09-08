package de.hhu.bsinfo.dxgraph.data;

import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Created by nothaas on 9/8/16.
 */
public class TestProperty extends Property<TestProperty> {

	static {
		PropertyManager.registerPropertyClass(TestProperty.class);
	}

	private int m_value;

	public TestProperty() {

	}

	public int getValue() {
		return m_value;
	}

	public void setValue(final int val) {
		m_value = val;
	}

	@Override
	public int exportObject(Exporter p_exporter, int p_size) {
		p_exporter.writeInt(m_value);

		return sizeofObject();
	}

	@Override
	public int importObject(Importer p_importer, int p_size) {
		m_value = p_importer.readInt();

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Integer.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}
}
