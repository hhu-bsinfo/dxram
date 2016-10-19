
package de.hhu.bsinfo.dxcompute.ms;

import com.google.gson.annotations.Expose;
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
public class TaskPayload implements Importable, Exportable {

	public static final short NUM_REQUIRED_SLAVES_ARBITRARY = 0;

	@Expose
	private short m_typeId = -1;
	@Expose
	private short m_subtypeId = -1;
	@Expose
	private short m_numRequiredSlaves = -1;

	/**
	 * Constructor
	 */
	public TaskPayload() {

	}

	/**
	 * Constructor
	 * Expecting a default constructor for the sub classes extending this
	 * base class, otherwise the createInstance call won't work.
	 * Make sure to register each task payload implementation prior usage.
	 *
	 * @param p_typeId       Type id
	 * @param p_subtypeId    Subtype id
	 * @param p_numReqSlaves Number of slaves required to run the task
	 */
	public TaskPayload(final short p_typeId, final short p_subtypeId, final short p_numReqSlaves) {
		m_typeId = p_typeId;
		m_subtypeId = p_subtypeId;
		m_numRequiredSlaves = p_numReqSlaves;
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
	 * Get the number of slaves required to run this task.
	 *
	 * @return Number of slaves this task requires.
	 */
	public short getNumRequiredSlaves() {
		return m_numRequiredSlaves;
	}

	/**
	 * Execute function to implement with the task/code to execute.
	 *
	 * @param p_ctx Context for this task containing DXRAM access and task parameters.
	 * @return Return code of your task. 0 on success, everything else indicates an error.
	 */
	public int execute(final TaskContext p_ctx) {
		return 0;
	}

	/**
	 * Handle a signal from the master
	 *
	 * @param p_signal Signal from the master
	 */
	public void handleSignal(final Signal p_signal) {

	}

	@Override
	public void importObject(final Importer p_importer) {
		m_numRequiredSlaves = p_importer.readShort();
	}

	@Override
	public void exportObject(final Exporter p_exporter) {
		p_exporter.writeShort(m_numRequiredSlaves);
	}

	@Override
	public int sizeofObject() {
		return Short.BYTES;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + m_typeId + ", " + m_subtypeId + ", " + m_numRequiredSlaves + "]";
	}
}
