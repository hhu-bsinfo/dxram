
package de.hhu.bsinfo.dxram.engine;

/**
 * Usually the concept does not allow a DXRAMService to use or even know
 * other DXRAMServices to keep things properly modularized. But there
 * might be exceptions (the less the better). One is a Job, which might execute
 * external/user code. To avoid writing wrappers and glue code to create another
 * API for the Jobs, we allow access to services inside the Jobs execution routine.
 * So far only the Jobs are using this. Do not use this anywhere else if there
 * aren't very good/similar reasons for it (refer to Jobs).
 * @author Stefan Nothaas <stefan.nothaas@hhu.de> 03.02.16
 */
public interface DXRAMServiceAccessor {
	/**
	 * Get a service from DXRAM.
	 * @param p_class
	 *            Class of the service to get. If the service has different implementations, use the common interface
	 *            or abstract class to get the registered instance.
	 * @param <T>
	 *            Class extending DXRAMService
	 * @return Reference to the service if available and enabled, null otherwise.
	 */
	<T extends AbstractDXRAMService> T getService(final Class<T> p_class);
}
