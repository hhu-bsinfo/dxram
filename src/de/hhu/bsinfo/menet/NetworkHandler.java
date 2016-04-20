
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.menet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.utils.log.LoggerInterface;
import de.hhu.bsinfo.utils.log.LoggerNull;

/**
 * Access the network through Java NIO
 * @author Florian Klein 18.03.2012
 * @author Marc Ewert 14.08.2014
 */
public final class NetworkHandler implements DataReceiver {

	// Attributes
	private static LoggerInterface m_loggerInterface;

	private final TaskExecutor m_messageCreatorExecutor;
	private final HashMap<Class<? extends AbstractMessage>, Entry> m_receivers;

	private final DefaultMessageHandler m_defaultMessageHandler;
	private final ExclusiveMessageHandler m_exclusiveMessageHandler;

	private MessageDirectory m_messageDirectory;
	private AbstractConnectionCreator m_connectionCreator;
	private ConnectionManager m_manager;
	private ReentrantLock m_receiversLock;

	private NodeMap m_nodeMap;

	private int m_numMessageHandlerThreads;

	// Constructors
	/**
	 * Creates an instance of NetworkHandler
	 * @param p_numMessageCreatorThreads
	 *            the number of message creatorn threads
	 * @param p_numMessageHandlerThreads
	 *            the number of default message handler (+ one exclusive message handler)
	 */
	public NetworkHandler(final int p_numMessageCreatorThreads, final int p_numMessageHandlerThreads) {
		final byte networkType;

		m_loggerInterface = new LoggerNull();

		m_numMessageHandlerThreads = p_numMessageHandlerThreads;

		NetworkHandler.getLogger().info(getClass().getSimpleName(),
				"Network: NetworkMessageCreator: Initialising " + p_numMessageCreatorThreads + " threads");
		m_messageCreatorExecutor = new TaskExecutor("NetworkMessageCreator", p_numMessageCreatorThreads);
		m_receivers = new HashMap<>();
		m_receiversLock = new ReentrantLock(false);

		m_defaultMessageHandler = new DefaultMessageHandler(p_numMessageHandlerThreads);

		m_exclusiveMessageHandler = new ExclusiveMessageHandler();
		m_exclusiveMessageHandler.setName("Network: ExclusiveMessageHandler");
		m_exclusiveMessageHandler.start();

		m_messageDirectory = new MessageDirectory();

		// Network Messages
		networkType = FlowControlMessage.TYPE;
		if (!m_messageDirectory.register(networkType, FlowControlMessage.SUBTYPE, FlowControlMessage.class)) {
			throw new IllegalArgumentException("Type and subtype for FlowControlMessage already in use.");
		}
	}

	/**
	 * Returns the LoggerInterface
	 * @return the LoggerInterface
	 */
	public static LoggerInterface getLogger() {
		return m_loggerInterface;
	}

	/**
	 * Sets the LoggerInterface
	 * @param p_logger
	 *            the LoggerInterface
	 */
	public void setLogger(final LoggerInterface p_logger) {
		m_loggerInterface = p_logger;

	}

	/**
	 * Returns the status of the network module
	 * @return the status
	 */
	public String getStatus() {
		String str = "";

		str += m_manager.getConnectionStatuses();
		str += m_connectionCreator.getSelectorStatus();

		return str;
	}

	/**
	 * Registers a message type
	 * @param p_type
	 *            the unique type
	 * @param p_subtype
	 *            the unique subtype
	 * @param p_class
	 *            the calling class
	 */
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		boolean ret = false;

		ret = m_messageDirectory.register(p_type, p_subtype, p_class);

		if (!ret) {
			m_loggerInterface.warn(getClass().getSimpleName(), "Registering network message " + p_class.getSimpleName()
					+ " for type " + p_type + " and subtype " + p_subtype + " failed, type and subtype already used.");
		}
	}

	// Methods
	/**
	 * Initializes the network handler
	 * @param p_ownNodeID
	 *            the own NodeID
	 * @param p_nodeMap
	 *            the node map
	 * @param p_incomingBufferSize
	 *            the size of incoming buffer
	 * @param p_outgoingBufferSize
	 *            the size of outgoing buffer
	 * @param p_numberOfBuffers
	 *            the number of bytes until a flow control message must be received to continue sending
	 * @param p_flowControlWindowSize
	 *            the maximal number of ByteBuffer to schedule for sending/receiving
	 * @param p_connectionTimeout
	 *            the connection timeout
	 */
	public void initialize(final short p_ownNodeID, final NodeMap p_nodeMap, final int p_incomingBufferSize,
			final int p_outgoingBufferSize, final int p_numberOfBuffers, final int p_flowControlWindowSize,
			final int p_connectionTimeout) {
		m_loggerInterface.trace(getClass().getSimpleName(), "Entering initialize");

		m_nodeMap = p_nodeMap;

		m_connectionCreator =
				new NIOConnectionCreator(m_messageCreatorExecutor, m_messageDirectory, m_nodeMap, p_incomingBufferSize,
						p_outgoingBufferSize,
						p_numberOfBuffers, p_flowControlWindowSize, p_connectionTimeout);
		m_connectionCreator.initialize(p_ownNodeID, p_nodeMap.getAddress(p_ownNodeID).getPort());
		m_manager = new ConnectionManager(m_connectionCreator, this);

		m_loggerInterface.trace(getClass().getSimpleName(), "Exiting initialize");
	}

	/**
	 * Activates the connection manager
	 */
	public void activateConnectionManager() {
		m_manager.activate();
	}

	/**
	 * Deactivates the connection manager
	 */
	public void deactivateConnectionManager() {
		m_manager.deactivate();
	}

	/**
	 * Closes the network handler
	 */
	public void close() {
		// Shutdown message creator(s)
		m_messageCreatorExecutor.shutdown();
		try {
			m_messageCreatorExecutor.awaitTermination();
			m_loggerInterface.info(getClass().getSimpleName(), "Shutdown of MessageCreator(s) successful.");
		} catch (final InterruptedException e) {
			m_loggerInterface.warn(getClass().getSimpleName(),
					"Could not wait for message creator thread pool to finish. Interrupted.");
		}

		// Shutdown default message handler(s)
		m_defaultMessageHandler.m_executor.shutdown();
		try {
			m_defaultMessageHandler.m_executor.awaitTermination();
			m_loggerInterface.info(getClass().getSimpleName(), "Shutdown of DefaultMessageHandler(s) successful.");
		} catch (final InterruptedException e) {
			m_loggerInterface.warn(getClass().getSimpleName(),
					"Could not wait for default message handler thread pool to finish. Interrupted.");
		}

		// Shutdown exclusive message handler
		m_exclusiveMessageHandler.interrupt();
		m_exclusiveMessageHandler.shutdown();
		try {
			m_exclusiveMessageHandler.join();
			m_loggerInterface.info(getClass().getSimpleName(), "Shutdown of ExclusiveMessageHandler successful.");
		} catch (final InterruptedException e) {
			m_loggerInterface.warn(getClass().getSimpleName(),
					"Could not wait for exclusive message handler to finish. Interrupted.");
		}

		// Close connection manager (shuts down selector thread, too)
		m_manager.close();
	}

	/**
	 * Registers a message receiver
	 * @param p_message
	 *            the message
	 * @param p_receiver
	 *            the receiver
	 */
	public void register(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			m_receiversLock.lock();
			entry = m_receivers.get(p_message);
			if (entry == null) {
				entry = new Entry();
				m_receivers.put(p_message, entry);
			}
			entry.add(p_receiver);

			m_loggerInterface.info(getClass().getSimpleName(),
					"Added new MessageReceiver " + p_receiver + " for " + p_message.getSimpleName());
			m_receiversLock.unlock();
		}
	}

	/**
	 * Unregisters a message receiver
	 * @param p_message
	 *            the message
	 * @param p_receiver
	 *            the receiver
	 */
	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			m_receiversLock.lock();
			entry = m_receivers.get(p_message);
			if (entry != null) {
				entry.remove(p_receiver);

				m_loggerInterface.info(getClass().getSimpleName(),
						"Removed MessageReceiver " + p_receiver + " from listening to " + p_message.getSimpleName());
			}
			m_receiversLock.unlock();
		}
	}

	/**
	 * Sends a message
	 * @param p_message
	 *            the message to send
	 * @return the status
	 */
	public int sendMessage(final AbstractMessage p_message) {
		AbstractConnection connection;

		p_message.beforeSend();

		m_loggerInterface.trace(getClass().getSimpleName(), "Entering sendMessage with: p_message=" + p_message);

		if (p_message != null) {
			/*
			 * NOTE:
			 * The following if statement is necessary to support looping back messages.
			 * Next to increasing performance it is not supported by the ConnectionManager
			 * to handle connections to itself. The problem is that in the loop-back-case
			 * there will be 2 connections to the local node ID:
			 * 1. The initial opened connection to the local server port and
			 * 2. The new incoming connection.
			 * So the ConnectionManager thinks he already has a connection to itself and
			 * the incoming connection will be discarded. Further, the incoming Messages
			 * can never be delivered.
			 */
			if (p_message.getDestination() == m_nodeMap.getOwnNodeID()) {
				// source is never set otherwise for loop back
				p_message.setSource(p_message.getDestination());
				newMessage(p_message);
			} else {
				try {
					connection = m_manager.getConnection(p_message.getDestination());
				} catch (final IOException e) {
					return -1;
				}
				try {
					if (null != connection) {
						connection.write(p_message);
					} else {
						return -1;
					}
				} catch (final IOException e) {
					return -2;
				}
			}
		}

		p_message.afterSend();

		m_loggerInterface.trace(getClass().getSimpleName(), "Exiting sendMessage");

		return 0;
	}

	/**
	 * Handles an incoming Message
	 * @param p_message
	 *            the incoming Message
	 */
	@Override
	public void newMessage(final AbstractMessage p_message) {
		m_loggerInterface.trace(getClass().getSimpleName(), "Received new message: " + p_message);

		if (p_message instanceof AbstractResponse) {
			RequestMap.fulfill((AbstractResponse) p_message);
		} else {
			if (!p_message.isExclusive()) {
				while (!m_defaultMessageHandler.newMessage(p_message, m_numMessageHandlerThreads * 2)) {
					// Too many pending messages -> wait
					if (m_manager.atLeastOneConnectionIsCongested()) {
						// All message handler could be blocked if a connection is congested (deadlock) -> add all (more
						// than limit) messages to task queue until flow control message arrives
						break;
					}
					Thread.yield();
				}
			} else {
				while (!m_exclusiveMessageHandler.newMessage(p_message, 4)) {
					// Too many pending messages -> wait
					if (m_manager.atLeastOneConnectionIsCongested()) {
						// All message handler could be blocked if a connection is congested (deadlock) -> add all (more
						// than limit) messages to task queue until flow control message arrives
						break;
					}
					Thread.yield();
				}
			}
		}
	}

	// Classes
	/**
	 * Distributes incoming messages
	 * @author Marc Ewert 17.09.2014
	 */
	private class DefaultMessageHandler implements Runnable {

		// Attributes
		private final ArrayDeque<AbstractMessage> m_defaultMessages;
		private ReentrantLock m_defaultMessagesLock;
		private final TaskExecutor m_executor;

		// Constructors
		/**
		 * Creates an instance of MessageHandler
		 * @param p_numMessageHandlerThreads
		 *            the number of default message handler
		 */
		DefaultMessageHandler(final int p_numMessageHandlerThreads) {
			m_defaultMessages = new ArrayDeque<>();
			m_defaultMessagesLock = new ReentrantLock(false);

			NetworkHandler.getLogger().info(getClass().getSimpleName(),
					"Network: DefaultMessageHandler: Initialising " + p_numMessageHandlerThreads + " threads");
			m_executor = new TaskExecutor("Network: DefaultMessageHandler", p_numMessageHandlerThreads);
		}

		// Methods
		/**
		 * Enqueue a new message for delivering
		 * @param p_message
		 *            the message
		 * @param p_maxMessages
		 *            the maximal number of pending messages
		 * @return whether the message was appended or not
		 */
		public boolean newMessage(final AbstractMessage p_message, final int p_maxMessages) {
			boolean ret = true;

			m_defaultMessagesLock.lock();
			if (m_defaultMessages.size() > p_maxMessages) {
				ret = false;
				m_defaultMessagesLock.unlock();
			} else {
				m_defaultMessages.offer(p_message);
				m_defaultMessagesLock.unlock();

				m_executor.execute(this);
			}

			return ret;
		}

		@Override
		public void run() {
			AbstractMessage message = null;
			Entry entry;

			m_defaultMessagesLock.lock();
			message = m_defaultMessages.poll();
			m_defaultMessagesLock.unlock();

			entry = m_receivers.get(message.getClass());

			if (entry != null) {
				entry.newMessage(message);
			} else {
				m_loggerInterface.error(getClass().getSimpleName(), "Default message queue is empty!");
			}
		}
	}

	/**
	 * Distributes incoming messages
	 * @author Marc Ewert 17.09.2014
	 */
	private class ExclusiveMessageHandler extends Thread {

		// Attributes
		private final ArrayDeque<AbstractMessage> m_exclusiveMessages;
		private ReentrantLock m_exclusiveMessagesLock;
		private Condition m_messageAvailable;
		private boolean m_shutdown;

		// Constructors
		/**
		 * Creates an instance of MessageHandler
		 */
		ExclusiveMessageHandler() {
			m_exclusiveMessages = new ArrayDeque<>();
			m_exclusiveMessagesLock = new ReentrantLock(false);
			m_messageAvailable = m_exclusiveMessagesLock.newCondition();
		}

		// Methods
		/**
		 * Closes the handler
		 */
		public void shutdown() {
			m_shutdown = true;
		}

		/**
		 * Enqueue a new message for delivering
		 * @param p_message
		 *            the message
		 * @param p_maxMessages
		 *            the maximal number of pending messages
		 * @return whether the message was appended or not
		 */
		public boolean newMessage(final AbstractMessage p_message, final int p_maxMessages) {
			boolean ret = true;

			m_exclusiveMessagesLock.lock();
			if (m_exclusiveMessages.size() > p_maxMessages) {
				ret = false;
				m_exclusiveMessagesLock.unlock();
			} else {
				m_exclusiveMessages.offer(p_message);

				if (m_exclusiveMessages.size() == 1) {
					m_messageAvailable.signal();
				}
				m_exclusiveMessagesLock.unlock();
			}

			return ret;
		}

		@Override
		public void run() {
			AbstractMessage message = null;
			Entry entry;

			while (!m_shutdown) {
				while (message == null) {
					m_exclusiveMessagesLock.lock();

					if (m_exclusiveMessages.size() == 0) {
						try {
							m_messageAvailable.await();
						} catch (final InterruptedException e) {
							m_shutdown = true;
							return;
						}
					}

					message = m_exclusiveMessages.poll();
					m_exclusiveMessagesLock.unlock();
				}

				entry = m_receivers.get(message.getClass());

				if (entry != null) {
					entry.newMessage(message);
				} else {
					m_loggerInterface.error(getClass().getSimpleName(), "Exclusive message queue is empty!");
				}
				message = null;
			}
		}
	}

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

	/**
	 * Wrapper class for message type - MessageReceiver pairs
	 * @author Florian Klein 23.07.2013
	 * @author Marc Ewert 14.08.2014
	 */
	private class Entry {

		// Attributes
		private final Collection<MessageReceiver> m_receivers;

		// Constructors
		/**
		 * Creates an instance of Entry
		 */
		Entry() {
			m_receivers = new CopyOnWriteArrayList<>();
		}

		// Methods
		/**
		 * Adds a MessageReceiver
		 * @param p_receiver
		 *            the MessageReceiver
		 */
		public void add(final MessageReceiver p_receiver) {
			m_receivers.add(p_receiver);
		}

		/**
		 * Removes a MessageReceiver
		 * @param p_receiver
		 *            the MessageReceiver
		 */
		public void remove(final MessageReceiver p_receiver) {
			m_receivers.remove(p_receiver);
		}

		/**
		 * Informs all MessageReceivers about a new message
		 * @param p_message
		 *            the message
		 */
		public void newMessage(final AbstractMessage p_message) {
			for (MessageReceiver receiver : m_receivers) {
				receiver.onIncomingMessage(p_message);
			}
		}
	}
}
