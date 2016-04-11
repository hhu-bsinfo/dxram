
package de.hhu.bsinfo.dxcompute.ms;

import java.util.Vector;

import de.hhu.bsinfo.dxram.DXRAM;
import de.hhu.bsinfo.dxram.logger.LoggerService;
import de.hhu.bsinfo.dxram.stats.StatisticsService;

public class TaskContext {
	private DXRAM m_dxram;
	private StatisticsService m_statisticsService;
	private LoggerService m_loggerService;

	private TaskStatisticsRecorderIDs m_statisticsRecorderIDs;

	private volatile boolean m_taskExecutionComplete;
	private volatile int m_taskExecutionReturnCode = -1;
	private Vector<TaskListener> m_completionListeners = new Vector<TaskListener>();

	public TaskContext() {

	}

	public void executePayload(final AbstractTaskPayload p_payload) {
		if (m_taskExecutionComplete) {
			m_loggerService.error(getClass(),
					"Executing task payload " + p_payload + " not possible, was already executed");
			return;
		}

		notifyListenersExecutionStarts();
		m_statisticsService.enter(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_execute);
		m_taskExecutionReturnCode = p_payload.execute();
		m_statisticsService.leave(m_statisticsRecorderIDs.m_id, m_statisticsRecorderIDs.m_operations.m_execute);
		m_taskExecutionComplete = true;
		notifyListenersExecutionCompleted();
	}

	/**
	 * Print all recorded statistics of this task to the console.
	 */
	public void printStatistics() {
		m_statisticsService.printStatistics(getClass());
	}

	/**
	 * Used by the pipeline set DXRAM and get services.
	 * @param p_dxram
	 *            DXRAM instance to use.
	 */
	void setDXRAM(final DXRAM p_dxram) {
		m_dxram = p_dxram;
		m_statisticsService = m_dxram.getService(StatisticsService.class);
		m_loggerService = m_dxram.getService(LoggerService.class);

		registerStatisticsOperations();
	}

	/**
	 * Register statistics to be recorded by the task itself (task only).
	 */
	private void registerStatisticsOperations() {
		m_statisticsRecorderIDs = new TaskStatisticsRecorderIDs();
		m_statisticsRecorderIDs.m_id = m_statisticsService.createRecorder(this.getClass());

		m_statisticsRecorderIDs.m_operations.m_execute = m_statisticsService
				.createOperation(m_statisticsRecorderIDs.m_id, TaskStatisticsRecorderIDs.Operations.MS_EXECUTE);
	}

	private void notifyListenersExecutionStarts() {
		for (TaskListener listener : m_completionListeners) {
			listener.taskBeforeExecution(this);
		}
	}

	private void notifyListenersExecutionCompleted() {
		for (TaskListener listener : m_completionListeners) {
			listener.taskBeforeExecution(this);
		}
	}
}
