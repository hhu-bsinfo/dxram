
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

/**
 * Base class for all tasks to be implemented. This holds optional data the task
 * needs for execution as well as the code getting executed for the task.
 * Make sure to register your newly created task payloads as well (refer to static functions in here).
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
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

	/**
	 * Constructor
	 * We are expecting a default constructor for the sub classes extending this
	 * base class, otherwise the createInstance call won't work.
	 * Make sure to register each task payload implementation prior usage.
	 * @param p_typeId
	 *            Type id
	 * @param p_subtypeId
	 *            Subtype id
	 */
	public AbstractTaskPayload(final short p_typeId, final short p_subtypeId) {
		m_typeId = p_typeId;
		m_subtypeId = p_subtypeId;
	}

	/**
	 * Create an instance of a registered task payload.
	 * @param p_typeId
	 *            Type id of the task payload.
	 * @param p_subtypeId
	 *            Subtype id of the task payload.
	 * @return New instance of the specified task payload.
	 * @throw RuntimeException If no task payload specifeid by the ids could be created.
	 */
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

	/**
	 * Get the list of registered task payload classes.
	 * @return List of registered task payload classes.
	 */
	public static Map<Integer, Class<? extends AbstractTaskPayload>> getRegisteredTaskPayloadClasses() {
		return ms_registeredTaskClasses;
	}

	/**
	 * Register a new task payload class.
	 * @param p_typeId
	 *            Type id for the class.
	 * @param p_subtypeId
	 *            Subtype id for the class.
	 * @param p_class
	 *            Class to register for the specified ids.
	 * @return True if registeration was successful, false otherwise.
	 */
	public static boolean registerTaskPayloadClass(final short p_typeId, final short p_subtypeId,
			final Class<? extends AbstractTaskPayload> p_class) {
		Class<? extends AbstractTaskPayload> clazz =
				ms_registeredTaskClasses.put(((p_typeId & 0xFFFF) << 16) | p_subtypeId, p_class);
		if (clazz != null) {
			return false;
		}

		return true;
	}

	/**
	 * Get the type id.
	 * @return Type id.
	 */
	public short getTypeId() {
		return m_typeId;
	}

	/**
	 * Get the subtype id.
	 * @return Subtype id.
	 */
	public short getSubtypeId() {
		return m_subtypeId;
	}

	/**
	 * Get the payload id assigned by the master node on submission.
	 * @return Payload id.
	 */
	public long getPayloadId() {
		return m_payloadId;
	}

	/**
	 * Get the compute group id this task is executed in.
	 * @return Compute group id.
	 */
	public int getComputeGroupId() {
		return m_computeGroupId;
	}

	/**
	 * Get the id of the slave that executes the task (0 based).
	 * @return Id of the slave executing the task.
	 */
	public int getSlaveId() {
		return m_slaveId;
	}

	/**
	 * Get the node ids of all slaves executing this task.
	 * @return Node ids of all slaves. Indexable by slave id.
	 */
	public short[] getSlaveNodeIds() {
		return m_slaveNodeIds;
	}

	/**
	 * Get the return codes of all slaves after execution finished.
	 * @return Array of return codes of all slaves after execution. Indexable by slave id.
	 */
	public int[] getExecutionReturnCodes() {
		return m_executionReturnCodes;
	}

	/**
	 * Execute function to implement with the task/code to execute.
	 * @param p_dxram
	 *            Service access of dxram to access all available services for your task.
	 * @return Return code of your task. 0 on success, everything else indicates an error.
	 */
	public abstract int execute(final DXRAMServiceAccessor p_dxram);

	/**
	 * Override this to allow terminal commands calling this to prompt for additional
	 * data to be entered as parameters for the payload.
	 * @param p_delegate
	 *            Delegate of the terminal.
	 * @return True on successful input, false on error.
	 */
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

	/**
	 * Set the payload id.
	 * @param p_payloadId
	 *            Payload id to set.
	 */
	void setPayloadId(final long p_payloadId) {
		m_payloadId = p_payloadId;
	}

	/**
	 * Set the compute group id.
	 * @param p_computeGroupId
	 *            Id to set.
	 */
	void setComputeGroupId(final int p_computeGroupId) {
		m_computeGroupId = p_computeGroupId;
	}

	/**
	 * Set the slave id.
	 * @param p_slaveId
	 *            Id to set.
	 */
	void setSlaveId(final int p_slaveId) {
		m_slaveId = p_slaveId;
	}

	/**
	 * Set the slave node ids.
	 * @param p_slaveNodeIds
	 *            Node ids to set. Indexable by slave id.
	 */
	void setSalves(final short[] p_slaveNodeIds) {
		m_slaveNodeIds = p_slaveNodeIds;
	}

	/**
	 * Set the execution return codes of all slaves.
	 * @param p_returnCodes
	 *            Return codes of all slaves. Indexable by slave id.
	 */
	void setExecutionReturnCodes(final int[] p_returnCodes) {
		m_executionReturnCodes = p_returnCodes;
	}
}
