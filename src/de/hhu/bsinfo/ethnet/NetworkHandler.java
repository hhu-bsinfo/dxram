
package de.hhu.bsinfo.ethnet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.hhu.bsinfo.ethnet.AbstractConnection.DataReceiver;
import de.hhu.bsinfo.utils.event.EventInterface;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Access the network through Java NIO
 *
 * @author Florian Klein 18.03.2012
 * @author Marc Ewert 14.08.2014
 */
public final class NetworkHandler implements DataReceiver {

	private static final Logger LOGGER = LogManager.getFormatterLogger(NetworkHandler.class.getSimpleName());

	// Attributes
	private static EventInterface m_eventInterface;

	private final HashMap<Class<? extends AbstractMessage>, Entry> m_receivers;

	private final DefaultMessageHandlerPool m_defaultMessageHandlerPool;
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
	 *
	 * @param p_numMessageHandlerThreads the number of default message handler (+ one exclusive message handler)
	 * @param p_requestMapSize           the number of entries in the request map
	 */
	public NetworkHandler(final int p_numMessageHandlerThreads, final int p_requestMapSize) {
		final byte networkType;

		RequestMap.initialize(p_requestMapSize);

		m_numMessageHandlerThreads = p_numMessageHandlerThreads;

		m_receivers = new HashMap<>();
		m_receiversLock = new ReentrantLock(false);

		m_defaultMessageHandlerPool = new DefaultMessageHandlerPool(p_numMessageHandlerThreads);

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
	 * Returns the EventInterface
	 *
	 * @return the EventInterface
	 */
	protected static EventInterface getEventHandler() {
		return m_eventInterface;
	}

	/**
	 * Sets the EventInterface
	 *
	 * @param p_event the EventInterface
	 */
	public void setEventHandler(final EventInterface p_event) {
		m_eventInterface = p_event;

	}

	/**
	 * Returns the status of the network module
	 *
	 * @return the status
	 */
	public String getStatus() {
		String str = "";

		str += m_manager.getConnectionStatuses();
		str += m_connectionCreator.getSelectorStatus();

		return str;
	}

	// class PrintThread extends Thread {
	// public void run() {
	// while (true) {
	// System.out.println(getStatus());
	// try {
	// Thread.sleep(4000);
	// } catch (InterruptedException e) {
	// e.printStackTrace();
	// }
	// }
	// }
	// }

	/**
	 * Registers a message type
	 *
	 * @param p_type    the unique type
	 * @param p_subtype the unique subtype
	 * @param p_class   the calling class
	 */
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class) {
		boolean ret = false;

		ret = m_messageDirectory.register(p_type, p_subtype, p_class);

		// #if LOGGER >= WARN
		if (!ret) {
			LOGGER.warn(
					"Registering network message %s for type %s and subtype %s failed, type and subtype already used",
					p_class.getSimpleName(), p_type, p_subtype);
		}
		// #endif /* LOGGER >= WARN */
	}

	// Methods

	/**
	 * Initializes the network handler
	 *
	 * @param p_ownNodeID                    the own NodeID
	 * @param p_nodeMap                      the node map
	 * @param p_incomingBufferSize           the size of incoming buffer
	 * @param p_outgoingBufferSize           the size of outgoing buffer
	 * @param p_numberOfBuffersPerConnection the number of bytes until a flow control message must be received to continue sending
	 * @param p_flowControlWindowSize        the maximal number of ByteBuffer to schedule for sending/receiving
	 * @param p_connectionTimeout            the connection timeout
	 */
	public void initialize(final short p_ownNodeID, final NodeMap p_nodeMap, final int p_incomingBufferSize,
			final int p_outgoingBufferSize, final int p_numberOfBuffersPerConnection, final int p_flowControlWindowSize,
			final int p_connectionTimeout) {

		// #if LOGGER == TRACE
		LOGGER.trace("Entering initialize");
		// #endif /* LOGGER == TRACE */

		m_nodeMap = p_nodeMap;

		m_connectionCreator = new NIOConnectionCreator(m_messageDirectory, m_nodeMap, p_ownNodeID, p_incomingBufferSize,
				p_outgoingBufferSize, p_numberOfBuffersPerConnection, p_flowControlWindowSize, p_connectionTimeout);
		m_connectionCreator.initialize(p_ownNodeID, p_nodeMap.getAddress(p_ownNodeID).getPort());
		m_manager = new ConnectionManager(m_connectionCreator, this);

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting initialize");
		// #endif /* LOGGER == TRACE */

		// (new PrintThread()).start();
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
		// Shutdown default message handler(s)
		m_defaultMessageHandlerPool.shutdown();

		// Shutdown exclusive message handler
		m_exclusiveMessageHandler.interrupt();
		m_exclusiveMessageHandler.shutdown();
		try {
			m_exclusiveMessageHandler.join();
			// #if LOGGER >= INFO
			LOGGER.info("Shutdown of ExclusiveMessageHandler successful");
			// #endif /* LOGGER >= INFO */
		} catch (final InterruptedException e) {
			// #if LOGGER >= WARN
			LOGGER.warn("Could not wait for exclusive message handler to finish. Interrupted");
			// #endif /* LOGGER >= WARN */
		}

		// Close connection manager (shuts down selector thread, too)
		m_manager.close();
	}

	/**
	 * Registers a message receiver
	 *
	 * @param p_message  the message
	 * @param p_receiver the receiver
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

			// #if LOGGER >= TRACE
			LOGGER.trace("Added new MessageReceiver %s for %s", p_receiver.getClass(), p_message.getSimpleName());
			// #endif /* LOGGER >= TRACE */
			m_receiversLock.unlock();
		}
	}

	/**
	 * Unregisters a message receiver
	 *
	 * @param p_message  the message
	 * @param p_receiver the receiver
	 */
	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			m_receiversLock.lock();
			entry = m_receivers.get(p_message);
			if (entry != null) {
				entry.remove(p_receiver);

				// #if LOGGER >= TRACE
				LOGGER.trace("Removed MessageReceiver %s from listening to %s",
						p_receiver, p_message.getSimpleName());
				// #endif /* LOGGER >= TRACE */
			}
			m_receiversLock.unlock();
		}
	}

	/**
	 * Connects a node.
	 *
	 * @param p_nodeID Node to connect
	 * @return the status
	 */
	public int connectNode(final short p_nodeID) {
		// #if LOGGER == TRACE
		LOGGER.trace("Entering connectNode with: p_nodeID=0x%X", p_nodeID);
		// #endif /* LOGGER == TRACE */

		try {
			m_manager.getConnection(p_nodeID);
		} catch (final IOException e) {
			// #if LOGGER >= DEBUG
			LOGGER.debug("IOException during connection lookup", e);
			// #endif /* LOGGER >= DEBUG */
			return -1;
		}

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting connectNode");
		// #endif /* LOGGER == TRACE */

		return 0;
	}

	/**
	 * Sends a message
	 *
	 * @param p_message the message to send
	 * @return the status
	 */
	public int sendMessage(final AbstractMessage p_message) {
		AbstractConnection connection;

		p_message.beforeSend();

		// #if LOGGER == TRACE
		LOGGER.trace("Entering sendMessage with: p_message=%s", p_message);
		// #endif /* LOGGER == TRACE */

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
					// #if LOGGER >= DEBUG
					LOGGER.debug("Connection invalid", e);
					// #endif /* LOGGER >= DEBUG */
					return -1;
				}
				try {
					if (null != connection) {
						connection.write(p_message);
					} else {
						// #if LOGGER >= DEBUG
						LOGGER.debug("Connection invalid");
						// #endif /* LOGGER >= DEBUG */
						return -1;
					}
				} catch (final NetworkException e) {
					// #if LOGGER >= DEBUG
					LOGGER.debug("Message invalid", e);
					// #endif /* LOGGER >= DEBUG */
					return -2;
				}
			}
		}

		p_message.afterSend();

		// #if LOGGER == TRACE
		LOGGER.trace("Exiting sendMessage");
		// #endif /* LOGGER == TRACE */

		return 0;
	}

	/**
	 * Handles an incoming Message
	 *
	 * @param p_message the incoming Message
	 */
	@Override
	public void newMessage(final AbstractMessage p_message) {
		// #if LOGGER == TRACE
		LOGGER.trace("Received new message: %s", p_message);
		// #endif /* LOGGER == TRACE */

		if (p_message instanceof AbstractResponse) {
			RequestMap.fulfill((AbstractResponse) p_message);
		} else {
			if (!p_message.isExclusive()) {
				int maxMessages = m_numMessageHandlerThreads * 2;
				while (!m_defaultMessageHandlerPool.newMessage(p_message, maxMessages)) {
					// Too many pending messages -> wait
					if (m_manager.atLeastOneConnectionIsCongested()) {
						// All message handler could be blocked if a connection is congested (deadlock) -> add all (more
						// than limit) messages to task queue until flow control message arrives
						maxMessages = Integer.MAX_VALUE;
						continue;
					}
					Thread.yield();
				}
			} else {
				int maxMessages = 4;
				while (!m_exclusiveMessageHandler.newMessage(p_message, maxMessages)) {
					// Too many pending messages -> wait
					if (m_manager.atLeastOneConnectionIsCongested()) {
						// All message handler could be blocked if a connection is congested (deadlock) -> add all (more
						// than limit) messages to task queue until flow control message arrives
						maxMessages = Integer.MAX_VALUE;
						continue;
					}
					Thread.yield();
				}
			}
		}
	}

	// Classes

	/**
	 * Distributes incoming default messages
	 *
	 * @author Kevin Beineke 19.07.2016
	 */
	private final class DefaultMessageHandlerPool {

		// Attributes
		private DefaultMessageHandler[] m_threads;
		private final ArrayDeque<AbstractMessage> m_defaultMessages;
		private ReentrantLock m_defaultMessagesLock;
		private Condition m_messageAvailable;

		// Constructors

		/**
		 * Creates an instance of DefaultMessageHandlerPool
		 *
		 * @param p_numMessageHandlerThreads the number of default message handler
		 */
		private DefaultMessageHandlerPool(final int p_numMessageHandlerThreads) {
			m_defaultMessages = new ArrayDeque<>();
			m_defaultMessagesLock = new ReentrantLock(false);
			m_messageAvailable = m_defaultMessagesLock.newCondition();

			// #if LOGGER >= INFO
			LOGGER.info("Network: DefaultMessageHandlerPool: Initialising %d threads", p_numMessageHandlerThreads);
			// #endif /* LOGGER >= INFO */

			DefaultMessageHandler t;
			m_threads = new DefaultMessageHandler[p_numMessageHandlerThreads];
			for (int i = 0; i < m_threads.length; i++) {
				t = new DefaultMessageHandler(m_defaultMessages, m_defaultMessagesLock, m_messageAvailable);
				t.setName("Network: DefaultMessageHandler " + (i + 1));
				m_threads[i] = t;
				t.start();
			}
		}

		// Methods

		/**
		 * Closes alle default message handler
		 */
		private void shutdown() {
			DefaultMessageHandler t;
			for (int i = 0; i < m_threads.length; i++) {
				t = m_threads[i];
				t.interrupt();
				t.shutdown();

				try {
					t.join();
					// #if LOGGER >= INFO
					LOGGER.info("Shutdown of DefaultMessageHandler %d successful", (i + 1));
					// #endif /* LOGGER >= INFO */
				} catch (final InterruptedException e) {
					// #if LOGGER >= WARN
					LOGGER.warn("Could not wait for default message handler to finish. Interrupted");
					// #endif /* LOGGER >= WARN */
				}
			}
		}

		/**
		 * Enqueue a new message for delivering
		 *
		 * @param p_message     the message
		 * @param p_maxMessages the maximal number of pending messages
		 * @return whether the message was appended or not
		 */
		private boolean newMessage(final AbstractMessage p_message, final int p_maxMessages) {
			boolean ret = true;

			m_defaultMessagesLock.lock();
			if (m_defaultMessages.size() > p_maxMessages) {
				ret = false;
				m_defaultMessagesLock.unlock();
			} else {
				m_defaultMessages.offer(p_message);

				m_messageAvailable.signal();
				m_defaultMessagesLock.unlock();
			}

			return ret;
		}
	}

	/**
	 * Executes incoming default messages
	 *
	 * @author Kevin Beineke 19.07.2016
	 */
	private final class DefaultMessageHandler extends Thread {

		// Attributes
		private ArrayDeque<AbstractMessage> m_defaultMessages;
		private ReentrantLock m_defaultMessagesLock;
		private Condition m_messageAvailable;
		private boolean m_shutdown;

		// Constructors

		/**
		 * Creates an instance of DefaultMessageHandler
		 *
		 * @param p_queue the message queue
		 * @param p_lock  the lock for accessing message queue
		 * @param p_cond  the condition for new messages
		 */
		private DefaultMessageHandler(final ArrayDeque<AbstractMessage> p_queue, final ReentrantLock p_lock,
				final Condition p_cond) {
			m_defaultMessages = p_queue;
			m_defaultMessagesLock = p_lock;
			m_messageAvailable = p_cond;
		}

		// Methods

		/**
		 * Closes the handler
		 */
		private void shutdown() {
			m_shutdown = true;
		}

		@Override
		public void run() {
			AbstractMessage message = null;
			Entry entry;

			while (!m_shutdown) {
				while (message == null) {
					m_defaultMessagesLock.lock();
					if (m_defaultMessages.size() == 0) {
						try {
							m_messageAvailable.await();
						} catch (final InterruptedException e) {
							m_defaultMessagesLock.unlock();
							return;
						}
					}

					message = m_defaultMessages.poll();
					m_defaultMessagesLock.unlock();
				}

				entry = m_receivers.get(message.getClass());

				if (entry != null) {
					entry.newMessage(message);
				} else {
					if (message == null) {
						// #if LOGGER >= ERROR
						LOGGER.error("Exclusive message queue is empty!");
						// #endif /* LOGGER >= ERROR */
					} else {
						if (message.getType() != (byte) 0) {
							// Type 0 are default messages and can be ignored

							// #if LOGGER >= ERROR
							LOGGER.error("No message receiver was registered for %d, %d!",
									message.getType(), message.getSubtype());
							// #endif /* LOGGER >= ERROR */
						}
					}
				}
				message = null;
			}
		}
	}

	/**
	 * Executes incoming exclusive messages
	 *
	 * @author Kevin Beineke 19.07.2016
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
		 *
		 * @param p_message     the message
		 * @param p_maxMessages the maximal number of pending messages
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
							m_exclusiveMessagesLock.unlock();
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
					if (message == null) {
						// #if LOGGER >= ERROR
						LOGGER.error("Exclusive message queue is empty!");
						// #endif /* LOGGER >= ERROR */
					} else {
						// #if LOGGER >= ERROR
						LOGGER.error("No message receiver was registered for %d, %d",
								message.getType(), message.getSubtype());
						// #endif /* LOGGER >= ERROR */
					}
				}
				message = null;
			}
		}
	}

	/**
	 * Methods for reacting on incoming Messages
	 *
	 * @author Florian Klein
	 *         09.03.2012
	 */
	public interface MessageReceiver {

		// Methods

		/**
		 * Handles an incoming Message
		 *
		 * @param p_message the Message
		 */
		void onIncomingMessage(AbstractMessage p_message);

	}

	/**
	 * Wrapper class for message type - MessageReceiver pairs
	 *
	 * @author Florian Klein 23.07.2013
	 * @author Marc Ewert 14.08.2014
	 */
	private class Entry {

		// Attributes
		private final CopyOnWriteArrayList<MessageReceiver> m_receivers;

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
		 *
		 * @param p_receiver the MessageReceiver
		 */
		public void add(final MessageReceiver p_receiver) {
			m_receivers.add(p_receiver);
		}

		/**
		 * Removes a MessageReceiver
		 *
		 * @param p_receiver the MessageReceiver
		 */
		public void remove(final MessageReceiver p_receiver) {
			m_receivers.remove(p_receiver);
		}

		/**
		 * Informs all MessageReceivers about a new message
		 *
		 * @param p_message the message
		 */
		public void newMessage(final AbstractMessage p_message) {
			for (int i = 0; i < m_receivers.size(); i++) {
				m_receivers.get(i).onIncomingMessage(p_message);
			}
		}
	}
}
