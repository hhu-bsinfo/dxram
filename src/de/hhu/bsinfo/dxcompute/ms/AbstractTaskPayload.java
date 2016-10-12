
package de.hhu.bsinfo.dxcompute.ms;

import de.hhu.bsinfo.dxram.engine.DXRAMServiceAccessor;
import de.hhu.bsinfo.utils.args.ArgumentList;
import de.hhu.bsinfo.utils.serialization.Exportable;
import de.hhu.bsinfo.utils.serialization.Exporter;
import de.hhu.bsinfo.utils.serialization.Importable;
import de.hhu.bsinfo.utils.serialization.Importer;

/**
 * Base class for all tasks to be implemented. This holds optional data the task
 * needs for execution as well as the code getting executed for the task.
 * Make sure to register your newly created task payloads as well (refer to static functions in here).
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 22.04.16
 */
public abstract class AbstractTaskPayload implements Importable, Exportable {

	private short m_typeId = -1;
	private short m_subtypeId = -1;
	private int m_payloadId = -1;
	private short m_computeGroupId = -1;
	private short m_slaveId = -1;
	// list of all slaves of the same compute group sorted by their slave id (indexable)
	private short[] m_slaveNodeIds = new short[0];
	private int[] m_executionReturnCodes = new int[0];

	private TaskPayloadSlaveInterface m_slaveInterface;

	/**
	 * Constructor
	 * Expecting a default constructor for the sub classes extending this
	 * base class, otherwise the createInstance call won't work.
	 * Make sure to register each task payload implementation prior usage.
	 *
	 * @param p_typeId    Type id
	 * @param p_subtypeId Subtype id
	 */
	public AbstractTaskPayload(final short p_typeId, final short p_subtypeId) {
		m_typeId = p_typeId;
		m_subtypeId = p_subtypeId;
	}

	/**
	 * Get the type id.
	 *
	 * @return Type id.
	 */
	public short getTypeId() {
		return m_typeId;
	}

	/**
	 * Get the subtype id.
	 *
	 * @return Subtype id.
	 */
	public short getSubtypeId() {
		return m_subtypeId;
	}

	/**
	 * Execute function to implement with the task/code to execute.
	 *
	 * @param p_dxram Service access of dxram to access all available services for your task.
	 * @return Return code of your task. 0 on success, everything else indicates an error.
	 */
	public abstract int execute(final DXRAMServiceAccessor p_dxram);

	/**
	 * Handle a signal from the master
	 *
	 * @param p_signal Signal from the master
	 */
	public abstract void handleSignal(final Signal p_signal);

	/**
	 * Override this to allow terminal commands calling this to get arguments
	 * required to run the task.
	 *
	 * @param p_argumentList List to add the arguments required to run.
	 */
	public void terminalCommandRegisterArguments(final ArgumentList p_argumentList) {

	}

	/**
	 * Override this to allow terminal commands to provide additional arguments inserted
	 * by the user.
	 *
	 * @param p_argumentList List of arguments from the terminal to lookup values for arguments
	 *                       required to run the task
	 */
	public void terminalCommandCallbackForArguments(final ArgumentList p_argumentList) {

	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		p_exporter.writeInt(m_payloadId);
		p_exporter.writeShort(m_slaveId);
		p_exporter.writeInt(m_slaveNodeIds.length);
		p_exporter.writeShorts(m_slaveNodeIds);
		p_exporter.writeInt(m_executionReturnCodes.length);
		p_exporter.writeInts(m_executionReturnCodes);
	}

	@Override
	public void importObject(final Importer p_importer) {
		m_payloadId = p_importer.readInt();
		m_slaveId = p_importer.readShort();
		m_slaveNodeIds = new short[p_importer.readInt()];
		p_importer.readShorts(m_slaveNodeIds);
		m_executionReturnCodes = new int[p_importer.readInt()];
		p_importer.readInts(m_executionReturnCodes);
	}

	@Override
	public int sizeofObject() {
		return Integer.BYTES + Short.BYTES + Integer.BYTES + m_slaveNodeIds.length * Short.BYTES + Integer.BYTES
				+ m_executionReturnCodes.length * Integer.BYTES;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + m_typeId + ", " + m_subtypeId + ", " + m_computeGroupId + ", "
				+ m_payloadId + ", " + m_slaveId
				+ "/" + m_slaveNodeIds.length + "]";
	}

	/**
	 * Set the payload id.
	 *
	 * @param p_payloadId Payload id to set.
	 */
	void setPayloadId(final int p_payloadId) {
		m_payloadId = p_payloadId;
	}

	/**
	 * Set the compute group id.
	 *
	 * @param p_computeGroupId Id to set.
	 */
	void setComputeGroupId(final short p_computeGroupId) {
		m_computeGroupId = p_computeGroupId;
	}

	/**
	 * Set the slave id.
	 *
	 * @param p_slaveId Id to set.
	 */
	void setSlaveId(final short p_slaveId) {
		m_slaveId = p_slaveId;
	}

	/**
	 * Set the slave node ids.
	 *
	 * @param p_slaveNodeIds Node ids to set. Indexable by slave id.
	 */
	void setSalves(final short[] p_slaveNodeIds) {
		m_slaveNodeIds = p_slaveNodeIds;
	}

	/**
	 * Set the execution return codes of all slaves.
	 *
	 * @param p_returnCodes Return codes of all slaves. Indexable by slave id.
	 */
	void setExecutionReturnCodes(final int[] p_returnCodes) {
		m_executionReturnCodes = p_returnCodes;
	}

	/**
	 * Set the slave interface for this task.
	 *
	 * @param p_slaveInterface Slave interface
	 */
	void setSlaveInterface(final TaskPayloadSlaveInterface p_slaveInterface) {
		m_slaveInterface = p_slaveInterface;
	}

	/**
	 * Get the payload id assigned by the master node on submission.
	 *
	 * @return Payload id.
	 */
	protected int getPayloadId() {
		return m_payloadId;
	}

	/**
	 * Get the compute group id this task is executed in.
	 *
	 * @return Compute group id.
	 */
	protected short getComputeGroupId() {
		return m_computeGroupId;
	}

	/**
	 * Get the id of the slave that executes the task (0 based).
	 *
	 * @return Id of the slave executing the task.
	 */
	protected short getSlaveId() {
		return m_slaveId;
	}

	/**
	 * Get the node ids of all slaves executing this task.
	 *
	 * @return Node ids of all slaves. Indexable by slave id.
	 */
	protected short[] getSlaveNodeIds() {
		return m_slaveNodeIds;
	}

	/**
	 * Get the return codes of all slaves after execution finished.
	 *
	 * @return Array of return codes of all slaves after execution. Indexable by slave id.
	 */
	protected int[] getExecutionReturnCodes() {
		return m_executionReturnCodes;
	}

	/**
	 * Send a signal from this task (and slave) to the master
	 *
	 * @param p_signal Signal to send
	 */
	protected void sendSignalToMaster(final Signal p_signal) {
		m_slaveInterface.sendSignalToMaster(p_signal);
	}
}
