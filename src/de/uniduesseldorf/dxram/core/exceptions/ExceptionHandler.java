
package de.uniduesseldorf.dxram.core.exceptions;

/**
 * Methods for reacting on exceptions
 * @author Florian Klein
 *         23.03.2012
 */
public interface ExceptionHandler {

	// Methods
	/**
	 * Handles an occurred LookupException
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(LookupException p_exception, ExceptionSource p_source, Object[] p_parameters);

	/**
	 * Handles an occurred NetworkException
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(NetworkException p_exception, ExceptionSource p_source, Object[] p_parameters);

	/**
	 * Handles an occurred DataException
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(ChunkException p_exception, ExceptionSource p_source, Object[] p_parameters);

	/**
	 * Handles an occurred LogException
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(PrimaryLogException p_exception, ExceptionSource p_source, Object[] p_parameters);

	/**
	 * Handles an occurred RecoverException
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(RecoveryException p_exception, ExceptionSource p_source, Object[] p_parameters);

	/**
	 * Handles an occurred ComponentCreationException
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(ComponentCreationException p_exception, ExceptionSource p_source, Object[] p_parameters);

	/**
	 * Handles an occurred Exception
	 * @param p_exception
	 *            the occurred exception
	 * @param p_source
	 *            the source of the exception
	 * @param p_parameters
	 *            the parameters of the method in which the exception occurred (optional)
	 * @return true if exception is successfully handled, false otherwise
	 */
	boolean handleException(Exception p_exception, ExceptionSource p_source, Object[] p_parameters);

	// Classes
	/**
	 * Methods witch throw an exception
	 * @author Florian Klein
	 *         23.03.2012
	 */
	public static enum ExceptionSource {

		// Constants
		DATA_INTERFACE, DHT_INTERFACE, DXRAM_CREATE_NEW_CHUNK, DXRAM_GET, DXRAM_GET_ASYNC, DXRAM_INITIALIZE, DXRAM_LOCK, DXRAM_MIGRATE, DXRAM_PUT,
		DXRAM_RECOVER_FROM_LOG, DXRAM_REMOVE, DXRAM_SUBSCRIBE, DXRAM_UNSUBSCRIBE, NETWORK_INTERFACE, RECOVERLOG_INTERFACE;

	}

}
