package de.hhu.bsinfo.dxcompute.ms;

/**
 * Interface to access the slave the task is running on
 *
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 16.07.16
 */
public interface TaskPayloadSlaveInterface {
	/**
	 * Send a signal to the master of the slave this task is running on.
	 *
	 * @param p_signal Signal to send
	 */
	void sendSignalToMaster(final Signal p_signal);
}
