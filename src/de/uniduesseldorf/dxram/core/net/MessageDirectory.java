
package de.uniduesseldorf.dxram.core.net;

import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.exceptions.NetworkException;

/**
 * Handles mapping from type and subtype to message class.
 * Every message class has to be registered here before it
 * can be used.
 * Message type 0 is dedicated to package intern used message
 * classes.
 * Currently used subtypes:
 * 1 - FlowcontrolMessage @see de.uniduesseldorf.dxram.core.net.AbstractConnection.FlowcontrolMessage
 * @author Marc Ewert 21.10.14
 */
public final class MessageDirectory {

	private static final Logger LOGGER = Logger.getLogger(MessageDirectory.class);

	private static Constructor<?>[][] m_constructors;

	static {
		m_constructors = new Constructor[0][0];
	}

	/**
	 * MessageDirectory is not designated to be instantiable
	 */
	private MessageDirectory() {}

	/**
	 * Registers a Message Type for receiving
	 * @param p_type
	 *            the type of the Message
	 * @param p_subtype
	 *            the subtype of the Message
	 * @param p_class
	 *            Message class
	 */
	public static synchronized void register(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		byte type;
		String text;
		Constructor<?>[][] typeArray;
		Constructor<?>[] subtypeArray;
		Constructor<?> constructor;

		if (contains(p_type, p_subtype)) {
			text = "Type " + p_type + " with subtype " + p_subtype + " is already registered";
			throw new IllegalArgumentException("Type " + p_type + " with subtype " + p_subtype
					+ " is already registered");
		}

		try {
			constructor = p_class.getDeclaredConstructor();
		} catch (final NoSuchMethodException e) {
			text = "Class " + p_class.getCanonicalName() + " has no default constructor";
			throw new IllegalArgumentException(text, e);
		}

		// enlarge array
		if (m_constructors.length <= p_type) {
			type = (byte) (p_type + 1);
			typeArray = new Constructor[type][];
			System.arraycopy(m_constructors, 0, typeArray, 0, m_constructors.length);
			m_constructors = typeArray;
		}

		// create new sub array when it is not existing until now
		if (m_constructors[p_type] == null) {
			m_constructors[p_type] = new Constructor<?>[p_subtype + 1];
		}

		// enlarge subtype array
		if (m_constructors[p_type].length <= p_subtype) {
			subtypeArray = new Constructor[p_subtype + 1];
			System.arraycopy(m_constructors[p_type], 0, subtypeArray, 0, m_constructors[p_type].length);
			m_constructors[p_type] = subtypeArray;
		}

		m_constructors[p_type][p_subtype] = constructor;

		LOGGER.info("Registered Type " + p_class.getSimpleName() + " with type " + p_type + " and subtype "
				+ p_subtype);
	}

	/**
	 * Lookup, if a specific message type is already registered
	 * @param p_type
	 *            the type of the Message
	 * @param p_subtype
	 *            the subtype of the Message
	 * @return true if registered
	 */
	public static boolean contains(final byte p_type, final byte p_subtype) {
		boolean result;

		if (m_constructors.length <= p_type) {
			result = false;
		} else if (m_constructors[p_type] == null || m_constructors[p_type].length <= p_subtype) {
			result = false;
		} else {
			result = m_constructors[p_type][p_subtype] != null;
		}

		return result;
	}

	/**
	 * Returns the constructor for a message class by its type and subtype
	 * @param p_type
	 *            the type of the Message
	 * @param p_subtype
	 *            the subtype of the Message
	 * @return message class constructor
	 */
	private static Constructor<?> getConstructor(final byte p_type, final byte p_subtype) {
		Constructor<?> result = null;

		if (contains(p_type, p_subtype)) {
			result = m_constructors[p_type][p_subtype];
		}

		return result;
	}

	/**
	 * Creates a Message instance for the type and subtype
	 * @param p_type
	 *            the type of the Message
	 * @param p_subtype
	 *            the subtype of the Message
	 * @return a new Message instance
	 * @throws de.uniduesseldorf.dxram.core.exceptions.NetworkException
	 *             if the instance could not be created
	 */
	public static AbstractMessage getInstance(final byte p_type, final byte p_subtype) throws NetworkException {
		AbstractMessage ret;
		Constructor<?> constructor;

		constructor = getConstructor(p_type, p_subtype);

		if (constructor == null) {
			throw new NetworkException("ERR::Could not create message instance: Message type (" + p_type + ":"
					+ p_subtype + ") not registered");
		}

		try {
			ret = (AbstractMessage) constructor.newInstance();
		} catch (final Exception e) {
			throw new NetworkException("ERR::Could not create message instance", e);
		}

		return ret;
	}
}
