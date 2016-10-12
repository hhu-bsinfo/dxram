
package de.hhu.bsinfo.dxcompute.ms;

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

	private static final short REQUIRED_NUM_SLAVES_ARBITRARY = 0;

	private short m_typeId = -1;
	private short m_subtypeId = -1;

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
	 * @param p_ctx Context for this task containing DXRAM access and task parameters.
	 * @return Return code of your task. 0 on success, everything else indicates an error.
	 */
	public abstract int execute(final TaskContext p_ctx);

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
	public void importObject(final Importer p_importer) {

	}

	@Override
	public void exportObject(final Exporter p_exporter) {

	}

	@Override
	public int sizeofObject() {
		return 0;
	}

	@Override
	public String toString() {
		return getClass().getSimpleName() + "[" + m_typeId + ", " + m_subtypeId + "]";
	}
}
