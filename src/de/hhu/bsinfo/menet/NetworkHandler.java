
package de.hhu.bsinfo.menet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
	private final TaskExecutor m_executor;
	private final HashMap<Class<? extends AbstractMessage>, Entry> m_receivers;

	private final MessageHandler m_messageHandler;

	private MessageDirectory m_messageDirectory;
	private ConnectionManager m_manager;
	private ReentrantLock m_receiversLock;

	private ReentrantLock m_lock;
	
	private NodeMap m_nodeMap = null;
	static LoggerInterface ms_logger = new LoggerNull();
	
	// Constructors
	/**
	 * Creates an instance of NetworkHandler
	 */
	public NetworkHandler(final int p_numMessageCreatorThreads, final int p_numMessageHandlerThreads) {
		final byte networkType;
		
		m_executor = new TaskExecutor("NetworkMessageCreator", p_numMessageCreatorThreads);
		m_receivers = new HashMap<>();
		m_receiversLock = new ReentrantLock(false);
		m_messageHandler = new MessageHandler(p_numMessageHandlerThreads);

		m_messageDirectory = new MessageDirectory();
		
		// Network Messages
		networkType = FlowControlMessage.TYPE;
		if (!m_messageDirectory.register(networkType, FlowControlMessage.SUBTYPE, FlowControlMessage.class)) {
			throw new IllegalArgumentException("Type and subtype for FlowControlMessage already in use.");
		}

		m_lock = new ReentrantLock(false);
	}
	
	public void setLogger(final LoggerInterface p_logger) {
		ms_logger = p_logger;
	}
	
	public ReentrantLock getExclusiveLock() {
		return m_messageHandler.m_exclusiveLock;
	}
	
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class)
	{
		boolean ret = false;
		m_lock.lock();
		ret = m_messageDirectory.register(p_type, p_subtype, p_class);
		m_lock.unlock();
		
		if (!ret) {
			ms_logger.warn(getClass().getSimpleName(), "Registering network message " + p_class.getSimpleName() + 
					" for type " + p_type + " and subtype " + p_subtype + " failed, type and subtype already used.");
		}
	}

	// Methods
	public void initialize(final short p_ownNodeID, final NodeMap p_nodeMap, final int p_maxOutstandingBytes, final int p_numberOfBuffers) {
		ms_logger.trace(getClass().getSimpleName(), "Entering initialize");
		
		m_lock.lock();
		
		m_nodeMap = p_nodeMap;
		
		AbstractConnectionCreator connectionCreator = new NIOConnectionCreator(m_executor, m_messageDirectory, m_nodeMap, p_maxOutstandingBytes, p_numberOfBuffers);
		connectionCreator.initialize(p_ownNodeID, p_nodeMap.getAddress(p_ownNodeID).getPort());
		m_manager = new ConnectionManager(connectionCreator, this);

		m_lock.unlock();

		ms_logger.trace(getClass().getSimpleName(), "Exiting initialize");
	}

	public void activateConnectionManager() {
		m_lock.lock();
		m_manager.activate();
		m_lock.unlock();
	}

	public void deactivateConnectionManager() {
		m_lock.lock();
		m_manager.deactivate();
		m_lock.unlock();
	}

	public void close() {
		ms_logger.trace(getClass().getSimpleName(), "Entering close");

		m_lock.lock();
		m_executor.shutdown();
		m_messageHandler.m_executor.shutdown();
		m_lock.unlock();

		ms_logger.trace(getClass().getSimpleName(), "Exiting close");
	}

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

			ms_logger.info(getClass().getSimpleName(), "new MessageReceiver");
			m_receiversLock.unlock();
		}
	}

	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			m_receiversLock.lock();
			entry = m_receivers.get(p_message);
			if (entry != null) {
				entry.remove(p_receiver);

				ms_logger.info(getClass().getSimpleName(), "MessageReceiver removed");
			}
			m_receiversLock.unlock();
		}
	}

	public int sendMessage(final AbstractMessage p_message) {
		AbstractConnection connection;

		p_message.beforeSend();
		
		ms_logger.trace(getClass().getSimpleName(), "Entering sendMessage with: p_message=" + p_message);

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

		ms_logger.trace(getClass().getSimpleName(), "Exiting sendMessage");
		
		return 0;
	}

	/**
	 * Handles an incoming Message
	 * @param p_message
	 *            the incoming Message
	 */
	@Override
	public void newMessage(final AbstractMessage p_message) {
		ms_logger.trace(getClass().getSimpleName(), "NewMessage: " + p_message);
		
		if (p_message instanceof AbstractResponse) {
			RequestMap.fulfill((AbstractResponse) p_message);
		} else {
			m_messageHandler.newMessage(p_message);
		}
	}

	// Classes
	/**
	 * Distributes incoming messages
	 * @author Marc Ewert 17.09.2014
	 */
	private class MessageHandler implements Runnable {

		// Attributes
		private final ArrayDeque<AbstractMessage> m_defaultMessages;
		private final ArrayDeque<AbstractMessage> m_exclusiveMessages;
		private int m_numMessageHandlerThreads;
		private final TaskExecutor m_executor;
		private ReentrantLock m_defaultMessagesLock;
		private ReentrantLock m_exclusiveLock;
		private ReentrantLock m_exclusiveMessagesLock;

		// Constructors
		/**
		 * Creates an instance of MessageHandler
		 */
		MessageHandler(final int p_numMessageHandlerThreads) {
			m_numMessageHandlerThreads = p_numMessageHandlerThreads;
			m_executor = new TaskExecutor("MessageHandler", m_numMessageHandlerThreads);
			m_defaultMessages = new ArrayDeque<>();
			m_exclusiveMessages = new ArrayDeque<>();
			m_defaultMessagesLock = new ReentrantLock(false);
			m_exclusiveLock = new ReentrantLock(false);
			m_exclusiveMessagesLock = new ReentrantLock(false);
		}

		// Methods
		/**
		 * Enqueue a new message for delivering
		 * @param p_message
		 *            the message
		 */
		public void newMessage(final AbstractMessage p_message) {
			// Limit number of tasks to NUMBER_OF_MESSAGE_HANDLER
			while (m_defaultMessages.size() + m_exclusiveMessages.size() > m_numMessageHandlerThreads * 2) {
				if (m_manager.atLeastOneConnectionIsCongested()) {
					// All message handler could be blocked if a connection is congested (deadlock) -> add all (more
					// than limit) messages to task queue until flow control message arrives
					break;
				}
				Thread.yield();
			}

			if (!p_message.isExclusive()) {
				m_defaultMessagesLock.lock();
				m_defaultMessages.offer(p_message);
				m_defaultMessagesLock.unlock();
			} else {
				m_exclusiveMessagesLock.lock();
				m_exclusiveMessages.offer(p_message);
				m_exclusiveMessagesLock.unlock();
			}

			m_executor.execute(this);
		}

		@Override
		public void run() {
			AbstractMessage message = null;
			boolean isExclusive = false;
			Entry entry;

			while (message == null) {
				if (m_exclusiveMessages.size() > m_defaultMessages.size() && m_exclusiveLock.tryLock()) {
					isExclusive = true;
					m_exclusiveMessagesLock.lock();
					message = m_exclusiveMessages.poll();
					m_exclusiveMessagesLock.unlock();
				} else {
					m_defaultMessagesLock.lock();
					message = m_defaultMessages.poll();
					m_defaultMessagesLock.unlock();
				}
			}

			entry = m_receivers.get(message.getClass());

			if (entry != null) {
				entry.newMessage(message);
			} else {
				System.out.println("Got no message!!!");
			}

			if (isExclusive) {
				m_exclusiveLock.unlock();
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
