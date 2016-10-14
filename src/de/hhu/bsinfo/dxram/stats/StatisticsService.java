
package de.hhu.bsinfo.dxram.stats;

import java.util.ArrayList;

import de.hhu.bsinfo.dxram.engine.AbstractDXRAMService;

/**
 * Exposing the component backend to the front with some
 * additional features like printing, filtering, ...
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 23.03.16
 */
public class StatisticsService extends AbstractDXRAMService {

	private StatisticsComponent m_statistics;

	/**
	 * Constructor
	 */
	public StatisticsService() {
		super("stats");
	}

	/**
	 * Create a new recorder to record statistics of a module.
	 *
	 * @param p_class Class to create a recorder for.
	 * @return Id of the newly created recorder (handle).
	 */
	public int createRecorder(final Class<?> p_class) {
		return m_statistics.createRecorder(p_class);
	}

	/**
	 * Add a new operation to an existing recorder.
	 *
	 * @param p_id            Recorder id to add a new operation to.
	 * @param p_operationName Name of the operation to create.
	 * @return Id of the operation created (handle).
	 */
	public int createOperation(final int p_id, final String p_operationName) {
		return m_statistics.createOperation(p_id, p_operationName);
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 */
	public void enter(final int p_recorderId, final int p_operationId) {
		m_statistics.enter(p_recorderId, p_operationId);
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 * @param p_val         Additional value to be added to the long counter.
	 */
	public void enter(final int p_recorderId, final int p_operationId, final long p_val) {
		m_statistics.enter(p_recorderId, p_operationId, p_val);
	}

	/**
	 * Call this when/before you start/enter the call/operation you want
	 * to record.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 * @param p_val         Additional value to be added to the double counter.
	 */
	public void enter(final int p_recorderId, final int p_operationId, final double p_val) {
		m_statistics.enter(p_recorderId, p_operationId, p_val);
	}

	/**
	 * Call this when/after you ended/left the call/operation.
	 *
	 * @param p_recorderId  Id of the recorder to record this call on.
	 * @param p_operationId Id of the operation to record.
	 */
	public void leave(final int p_recorderId, final int p_operationId) {
		m_statistics.leave(p_recorderId, p_operationId);
	}

	/**
	 * Get a list of all registered/created recorders.
	 *
	 * @return List of StatisticsRecorders.
	 */
	public ArrayList<StatisticsRecorder> getRecorders() {
		return m_statistics.getRecorders();
	}

	/**
	 * Get a specific recorder.
	 *
	 * @param p_class Class this recorder was created for.
	 * @return StatisticsRecorder if one was created for that class or null.
	 */
	public StatisticsRecorder getRecorder(final Class<?> p_class) {
		return m_statistics.getRecorder(p_class);
	}

	/**
	 * Print the statistics of all created recorders to the console.
	 */
	public void printStatistics() {
		for (StatisticsRecorder recorder : m_statistics.getRecorders()) {
			System.out.println(recorder);
		}
	}

	/**
	 * Print the statistics of a specific recorder to the console.
	 *
	 * @param p_class Class this recorder was created for.
	 */
	public void printStatistics(final Class<?> p_class) {
		StatisticsRecorder recorder = m_statistics.getRecorder(p_class);
		if (recorder != null) {
			System.out.println(recorder);
		}
	}

	@Override
	protected void registerDefaultSettingsService(final Settings p_settings) {

	}

	@Override
	protected boolean startService(final de.hhu.bsinfo.dxram.engine.DXRAMEngine.Settings p_engineSettings,
			final Settings p_settings) {
		m_statistics = getComponent(StatisticsComponent.class);

		return true;
	}

	@Override
	protected boolean shutdownService() {
		m_statistics = null;
		return true;
	}

}
