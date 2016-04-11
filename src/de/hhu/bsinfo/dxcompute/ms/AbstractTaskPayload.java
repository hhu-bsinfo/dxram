
package de.hhu.bsinfo.dxcompute.ms;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

public abstract class AbstractTaskPayload implements Importable, Exportable {

	protected static Map<Integer, Class<? extends AbstractTaskPayload>> ms_registeredTaskClasses =
			new HashMap<Integer, Class<? extends AbstractTaskPayload>>();

	private int m_typeId = -1;
	private long m_payloadId = -1;
	private int m_slaveId = -1;

	public static AbstractTaskPayload createInstance(final int p_typeId) {
		Class<? extends AbstractTaskPayload> clazz = ms_registeredTaskClasses.get(p_typeId);
		if (clazz == null) {
			return null;
		}

		try {
			Constructor<? extends AbstractTaskPayload> ctor = clazz.getConstructor();
			return ctor.newInstance();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			return null;
		}
	}

	protected static void registerTaskClass(final int p_id, final Class<? extends AbstractTaskPayload> p_class) {
		Class<? extends AbstractTaskPayload> clazz = ms_registeredTaskClasses.put(p_id, p_class);
		if (clazz != null) {
			throw new RuntimeException("Cannot register class " + p_class.getName() + " for task id " + p_id
					+ ", already used by class " + clazz.getName());
		}
	}

	public AbstractTaskPayload(final int p_typeId) {
		m_typeId = p_typeId;
	}

	public int getTaskTypeId() {
		return m_typeId;
	}

	public long getPayloadId() {
		return m_payloadId;
	}

	public int getSlaveId() {
		return m_slaveId;
	}

	public abstract int execute(final DXRAM p_dxram);

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		p_exporter.writeLong(m_payloadId);
		p_exporter.writeInt(m_slaveId);

		return sizeofObject();
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		m_payloadId = p_importer.readLong();
		m_slaveId = p_importer.readInt();

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Long.BYTES + Integer.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}

	@Override
	public String toString() {
		return "AbstractTaskPayload " + m_payloadId;
	}

	void setPayloadId(final long p_payloadId) {
		m_payloadId = p_payloadId;
	}

	void setSlaveId(final int p_slaveId) {
		m_slaveId = p_slaveId;
	}
}
