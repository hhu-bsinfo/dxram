
package de.uniduesseldorf.dxram.core.monitoring;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.exceptions.DXRAMException;

/**
 * Implements the Monitoring-Service
 * @author Florian Klein 06.08.2012
 */
public final class MonitoringHandler implements MonitoringInterface {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(MonitoringHandler.class);

	// Constructors
	/**
	 * Creates an instance of RecoveryHandler
	 */
	public MonitoringHandler() {}

	// Methods
	/**
	 * Initializes component<br>
	 * Should be called before any other method call of the component
	 * @throws DXRAMException
	 *             if the component could not be initialized
	 */
	@Override
	public void initialize() throws DXRAMException {
		LOGGER.trace("Entering initialize");

		LOGGER.trace("Exiting initialize");
	}

	/**
	 * Closes Component und frees unused ressources
	 */
	@Override
	public void close() {
		LOGGER.trace("Entering close");

		LOGGER.trace("Exiting close");
	}

}
