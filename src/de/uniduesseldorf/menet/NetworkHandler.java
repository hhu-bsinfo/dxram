
package de.uniduesseldorf.menet;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.log.LogMessages;

import de.uniduesseldorf.menet.AbstractConnection.DataReceiver;
import de.uniduesseldorf.utils.StatisticsManager;

/**
 * Access the network through Java NIO
 * @author Florian Klein 18.03.2012
 * @author Marc Ewert 14.08.2014
 */
public final class NetworkHandler implements NetworkInterface, DataReceiver {

	// Constants
	private static final Logger LOGGER = Logger.getLogger(NetworkHandler.class);

	// Attributes
	private final TaskExecutor m_executor;
	private final HashMap<Class<? extends AbstractMessage>, Entry> m_receivers;

	private final MessageHandler m_messageHandler;

	private MessageDirectory m_messageDirectory;
	private ConnectionManager m_manager;
	private ReentrantLock m_receiversLock;

	private ReentrantLock m_lock;
	
	private short m_ownNodeID;

	// Constructors
	/**
	 * Creates an instance of NetworkHandler
	 */
	public NetworkHandler(final int p_numTaskThreadsNetworkHandler, final int p_numTaskThreadsMessageHandler, 
			final boolean p_enableStatisticsThroughput, final boolean p_enableStatisticsRequests) {
		final byte networkType;
		
		m_executor = new TaskExecutor("NetworkHandler", p_numTaskThreadsNetworkHandler);
		m_receivers = new HashMap<>();
		m_receiversLock = new ReentrantLock(false);
		m_messageHandler = new MessageHandler(p_numTaskThreadsMessageHandler);

		m_messageDirectory = new MessageDirectory();
		
		// Network Messages
		networkType = NIOConnectionCreator.FlowControlMessage.TYPE;
		m_messageDirectory.register(networkType, NIOConnectionCreator.FlowControlMessage.SUBTYPE, NIOConnectionCreator.FlowControlMessage.class);

		m_lock = new ReentrantLock(false);
		
		m_ownNodeID = NodeID.INVALID_ID;
		
		if (p_enableStatisticsThroughput) {
			StatisticsManager.registerStatistic("Throughput", ThroughputStatistic.getInstance());
		}
		if (p_enableStatisticsRequests) {
			StatisticsManager.registerStatistic("Request", RequestStatistic.getInstance());
		}
	}
	
	public void registerMessageType(final byte p_type, final byte p_subtype, final Class<?> p_class)
	{
		m_lock.lock();
		m_messageDirectory.register(p_type, p_subtype, p_class);
		m_lock.unlock();
	}

	// Methods
	public void initialize(final short p_ownNodeID, final NodeMap p_nodeMap, final int p_maxOutstandingBytes) {
		LOGGER.trace("Entering initialize");

		m_lock.lock();
		
		m_manager = new ConnectionManager(new NIOConnectionCreator(m_executor, m_messageDirectory, p_nodeMap, p_maxOutstandingBytes));

		m_lock.unlock();

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public void activateConnectionManager() {
		m_lock.lock();
		m_manager.activate();
		m_lock.unlock();
	}

	@Override
	public void deactivateConnectionManager() {
		m_lock.lock();
		m_manager.deactivate();
		m_lock.unlock();
	}

	public void close() {
		LOGGER.trace("Entering close");

		m_lock.lock();
		m_executor.shutdown();
		m_messageHandler.m_executor.shutdown();
		m_lock.unlock();

		LOGGER.trace("Exiting close");
	}

	@Override
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

			LOGGER.info("new MessageReceiver");
			m_receiversLock.unlock();
		}
	}

	@Override
	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			m_receiversLock.lock();
			entry = m_receivers.get(p_message);
			if (entry != null) {
				entry.remove(p_receiver);

				LOGGER.info("MessageReceiver removed");
			}
			m_receiversLock.unlock();
		}
	}

	@Override
	public void sendMessage(final AbstractMessage p_message) throws NetworkException {
		AbstractConnection connection;

		// LOGGER.trace("Entering sendMessage with: p_message=" + p_message);

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
			if (p_message.getDestination() == m_ownNodeID) {
				newMessage(p_message);
			} else {
				try {
					connection = m_manager.getConnection(p_message.getDestination());
					if (null != connection) {
						connection.write(p_message);
					} else {
						throw new NetworkException("Error while accessing network connection");
					}
				} catch (final IOException e) {
					throw new NetworkException("Error while accessing network connection", e);
				}
			}
		}

		// LOGGER.trace("Exiting sendMessage");
	}

	/**
	 * Handles an incoming Message
	 * @param p_message
	 *            the incoming Message
	 */
	@Override
	public void newMessage(final AbstractMessage p_message) {
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
		private final TaskExecutor m_executor;
		private ReentrantLock m_messagesLock;
		private ReentrantLock m_exclusiveLock;

		// Constructors
		/**
		 * Creates an instance of MessageHandler
		 */
		MessageHandler(final int p_numMessageHandlerThreads) {
			m_executor = new TaskExecutor("MessageHandler", p_numMessageHandlerThreads);
			m_defaultMessages = new ArrayDeque<>();
			m_exclusiveMessages = new ArrayDeque<>();
			m_messagesLock = new ReentrantLock(false);
			m_exclusiveLock = new ReentrantLock(false);
		}

		// Methods
		/**
		 * Enqueue a new message for delivering
		 * @param p_message
		 *            the message
		 */
		public void newMessage(final AbstractMessage p_message) {
			// TODO LogMessage.TYPE has to vanish
			if (p_message.getType() == LogMessages.TYPE) {
				m_messagesLock.lock();
				m_exclusiveMessages.offer(p_message);
				m_messagesLock.unlock();
			} else {
				m_messagesLock.lock();
				m_defaultMessages.offer(p_message);
				m_messagesLock.unlock();
			}

			m_executor.execute(this);
		}

		@Override
		public void run() {
			AbstractMessage message;
			Entry entry;

			m_messagesLock.lock();
			message = m_defaultMessages.poll();
			if (message == null) {
				message = m_exclusiveMessages.poll();
				m_exclusiveLock.lock();//
			}
			m_messagesLock.unlock();

			m_receiversLock.lock();
			entry = m_receivers.get(message.getClass());
			m_receiversLock.unlock();

			if (entry != null) {
				entry.newMessage(message);
				if (message.getType() == LogMessages.TYPE) {
					m_exclusiveLock.unlock();
				}
			}
		}
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
