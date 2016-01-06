
package de.uniduesseldorf.menet;

/**
 * Methods for accessing the network
 * @author Florian Klein
 *         09.03.2012
 */
public interface NetworkInterface {

	// Methods
	/**
	 * Activates the ConnectionManager
	 */
	void activateConnectionManager();

	/**
	 * Deactivates the ConnectionManager
	 */
	void deactivateConnectionManager();
	
	/**
	 * Register a new type of message with the network.
	 * @param p_type Type identifier.
	 * @param p_subtype Subtype identifier.
	 * @param p_class Class to register for this type.
	 */
	void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class);

	/**
	 * Sends a Message
	 * @param p_message
	 *            the Message
	 * @returns 0 if successful, -1 could not connect to destination, -2 sending message failed
	 */
	int sendMessage(final AbstractMessage p_message);

	/**
	 * Forward the message to the given destination.
	 * @param p_destination Target destination to forward to.
	 * @param p_message Message to forward.
	 * @returns 0 if successful, -1 could not connect to destination, -2 sending message failed
	 */
	int forwardMessage(final short p_destination, final AbstractMessage p_message);
	
	/**
	 * Registers a MessageReceiver for a message class
	 * @param p_message
	 *            the message class
	 * @param p_receiver
	 *            the MessageReceiver
	 */
	void register(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver);

	/**
	 * Unregister a MessageReceiver for a message class
	 * @param p_message
	 *            the message class
	 * @param p_receiver
	 *            the MessageReceiver
	 */
	void unregister(Class<? extends AbstractMessage> p_message, MessageReceiver p_receiver);

	// Classes
	/**
	 * Methods for reacting on incoming Messages
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public interface MessageReceiver {

		// Methods
		/**
		 * Handles an incoming Message
		 * @param p_message
		 *            the Message
		 */
		void onIncomingMessage(AbstractMessage p_message);

	}

}
