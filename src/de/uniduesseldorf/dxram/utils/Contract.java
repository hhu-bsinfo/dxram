
package de.uniduesseldorf.dxram.utils;

import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Methods for checking objects and conditions
 * @author Florian Klein
 *         09.03.2012
 */
public final class Contract {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(Contract.class);

	// Constructors
	/**
	 * Creates an instance of Contract
	 */
	private Contract() {}

	// Methods
	/**
	 * Checks if the condition is true and throws an ContractException otherwise
	 * @param p_condition
	 *            the condition
	 * @throws ContractException
	 */
	public static void check(final boolean p_condition) {
		if (!p_condition) {
			LOGGER.fatal("FATAL::check failed [at " + Thread.currentThread().getStackTrace()[1] + "]");

			throw new ContractException();
		}
	}

	/**
	 * Checks if the condition is true and throws an ContractException otherwise
	 * @param p_condition
	 *            the condition
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 */
	public static void check(final boolean p_condition, final String p_message) {
		if (!p_condition) {
			LOGGER.fatal("FATAL::check failed [at " + Thread.currentThread().getStackTrace()[1] + "]: " + p_message);

			throw new ContractException(p_message);
		}
	}

	/**
	 * Checks if the object is not null and throws an ContractException otherwise
	 * @param p_object
	 *            the object
	 * @throws ContractException
	 */
	public static void checkNotNull(final Object p_object) {
		if (p_object == null) {
			LOGGER.fatal("FATAL::null check failed [at " + Thread.currentThread().getStackTrace()[1] + "]");

			throw new ContractException();
		}
	}

	/**
	 * Checks if the object is not null and throws an ContractException otherwise
	 * @param p_object
	 *            the object
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 */
	public static void checkNotNull(final Object p_object, final String p_message) {
		if (p_object == null) {
			LOGGER.fatal("FATAL::null check failed [at " + Thread.currentThread().getStackTrace()[1] + "]: "
					+ p_message);

			throw new ContractException(p_message);
		}
	}

	/**
	 * Checks if the collection is not empty and throws an ContractException otherwise
	 * @param p_collection
	 *            the collection
	 * @throws ContractException
	 */
	public static void checkNotEmpty(final Collection<?> p_collection) {
		if (p_collection.isEmpty()) {
			LOGGER.fatal("FATAL::empty check failed [at " + Thread.currentThread().getStackTrace()[1] + "]");

			throw new ContractException();
		}
	}

	/**
	 * Checks if the collection is not empty and throws an ContractException otherwise
	 * @param p_collection
	 *            the collection
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 */
	public static void checkNotEmpty(final Collection<?> p_collection, final String p_message) {
		if (p_collection.isEmpty()) {
			LOGGER.fatal("FATAL::empty check failed [at " + Thread.currentThread().getStackTrace()[1] + "]: "
					+ p_message);

			throw new ContractException(p_message);
		}
	}

	/**
	 * Checks if the map is not empty and throws an ContractException otherwise
	 * @param p_map
	 *            the map
	 * @throws ContractException
	 */
	public static void checkNotEmpty(final Map<?, ?> p_map) {
		if (p_map.isEmpty()) {
			LOGGER.fatal("FATAL::empty check failed [at " + Thread.currentThread().getStackTrace()[1] + "]");

			throw new ContractException();
		}
	}

	/**
	 * Checks if the map is not empty and throws an ContractException otherwise
	 * @param p_map
	 *            the map
	 * @param p_message
	 *            the exception message
	 * @throws ContractException
	 */
	public static void checkNotEmpty(final Map<?, ?> p_map, final String p_message) {
		if (p_map.isEmpty()) {
			LOGGER.fatal("FATAL::empty check failed [at " + Thread.currentThread().getStackTrace()[1] + "]: "
					+ p_message);

			throw new ContractException(p_message);
		}
	}

	// Classes
	/**
	 * Exception for breaking a contract
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public static final class ContractException extends RuntimeException {

		// Constants
		private static final long serialVersionUID = -3167467198371576288L;

		// Constructors
		/**
		 * Creates an instance of ContractException
		 */
		private ContractException() {
			super();
		}

		/**
		 * Creates an instance of ContractException
		 * @param p_message
		 *            the message
		 */
		private ContractException(final String p_message) {
			super(p_message);
		}

	}

}
