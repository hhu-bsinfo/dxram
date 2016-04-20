
package de.hhu.bsinfo.dxcompute.ms;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.dxram.term.TerminalDelegate;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

public abstract class AbstractTaskPayload implements Importable, Exportable {

	protected static Map<Integer, Class<? extends AbstractTaskPayload>> ms_registeredTaskClasses =
			new HashMap<Integer, Class<? extends AbstractTaskPayload>>();

	private short m_typeId = -1;
	private short m_subtypeId = -1;
	private long m_payloadId = -1;
	private int m_computeGroupId = -1;
	private int m_slaveId = -1;
	// list of all slaves of the same compute group sorted by their slave id (indexable)
	private short[] m_slaveNodeIds = new short[0];
	private int[] m_executionReturnCodes = new int[0];

	public static AbstractTaskPayload createInstance(final short p_typeId, final short p_subtypeId) {
		Class<? extends AbstractTaskPayload> clazz =
				ms_registeredTaskClasses.get(((p_typeId & 0xFFFF) << 16) | p_subtypeId);
		if (clazz == null) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId
							+ ", not registered.");
		}

		try {
			Constructor<? extends AbstractTaskPayload> ctor = clazz.getConstructor();
			return ctor.newInstance();
		} catch (final NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException
				| IllegalArgumentException | InvocationTargetException e) {
			throw new RuntimeException(
					"Cannot create instance of TaskPayload with id " + p_typeId + "|" + p_subtypeId + ": "
							+ e.getMessage());
		}
	}

	public static Map<Integer, Class<? extends AbstractTaskPayload>> getRegisteredTaskPayloadClasses() {
		return ms_registeredTaskClasses;
	}

	protected static boolean registerTaskPayloadClass(final short p_typeId, final short p_subtypeId,
			final Class<? extends AbstractTaskPayload> p_class) {
		Class<? extends AbstractTaskPayload> clazz =
				ms_registeredTaskClasses.put(((p_typeId & 0xFFFF) << 16) | p_subtypeId, p_class);
		if (clazz != null) {
			return false;
		}

		return true;
	}

	// we are expecting a default constructor for the class extending this one
	// (see create instance code)
	// also make sure to register each task payload implementation prior usage
	public AbstractTaskPayload(final short p_typeId, final short p_subtypeId) {
		m_typeId = p_typeId;
		m_subtypeId = p_subtypeId;
	}

	public short getTypeId() {
		return m_typeId;
	}

	public short getSubtypeId() {
		return m_subtypeId;
	}

	public long getPayloadId() {
		return m_payloadId;
	}

	public int getComputeGroupId() {
		return m_computeGroupId;
	}

	public int getSlaveId() {
		return m_slaveId;
	}

	public short[] getSlaveNodeIds() {
		return m_slaveNodeIds;
	}

	public int[] getExecutionReturnCodes() {
		return m_executionReturnCodes;
	}

	public abstract int execute(final DXRAMServiceAccessor p_dxram);

	// override this to allow terminal commands calling this to prompt for parameters
	// that can be passed along with the payload
	public boolean terminalCommandCallbackForParameters(final TerminalDelegate p_delegate) {
		return true;
	}

	@Override
	public int exportObject(final Exporter p_exporter, final int p_size) {
		p_exporter.writeLong(m_payloadId);
		p_exporter.writeInt(m_slaveId);
		p_exporter.writeInt(m_slaveNodeIds.length);
		p_exporter.writeShorts(m_slaveNodeIds);
		p_exporter.writeInt(m_executionReturnCodes.length);
		p_exporter.writeInts(m_executionReturnCodes);

		return sizeofObject();
	}

	@Override
	public int importObject(final Importer p_importer, final int p_size) {
		m_payloadId = p_importer.readLong();
		m_slaveId = p_importer.readInt();
		m_slaveNodeIds = new short[p_importer.readInt()];
		p_importer.readShorts(m_slaveNodeIds);
		m_executionReturnCodes = new int[p_importer.readInt()];
		p_importer.readInts(m_executionReturnCodes);

		return sizeofObject();
	}

	@Override
	public int sizeofObject() {
		return Long.BYTES + Integer.BYTES + Integer.BYTES + m_slaveNodeIds.length * Short.BYTES + Integer.BYTES
				+ m_executionReturnCodes.length * Integer.BYTES;
	}

	@Override
	public boolean hasDynamicObjectSize() {
		return false;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + m_typeId + ", " + m_subtypeId + ", " + m_computeGroupId + ", "
				+ m_payloadId + ", " + m_slaveId
				+ "/" + m_slaveNodeIds.length + "]";
	}

	void setPayloadId(final long p_payloadId) {
		m_payloadId = p_payloadId;
	}

	void setComputeGroupId(final int p_computeGroupId) {
		m_computeGroupId = p_computeGroupId;
	}

	void setSlaveId(final int p_slaveId) {
		m_slaveId = p_slaveId;
	}

	void setSalves(final short[] p_slaveNodeIds) {
		m_slaveNodeIds = p_slaveNodeIds;
	}

	void setExecutionReturnCodes(final int[] p_returnCodes) {
		m_executionReturnCodes = p_returnCodes;
	}
}
