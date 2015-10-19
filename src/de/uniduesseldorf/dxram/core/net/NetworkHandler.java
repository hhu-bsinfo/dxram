
package de.uniduesseldorf.dxram.core.net;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.log4j.Logger;

import de.uniduesseldorf.dxram.core.api.Core;
import de.uniduesseldorf.dxram.core.api.NodeID;
import de.uniduesseldorf.dxram.core.api.config.Configuration.ConfigurationConstants;
import de.uniduesseldorf.dxram.core.chunk.ChunkMessages;
import de.uniduesseldorf.dxram.core.exceptions.NetworkException;
import de.uniduesseldorf.dxram.core.log.LogMessages;
import de.uniduesseldorf.dxram.core.lookup.LookupMessages;
import de.uniduesseldorf.dxram.core.net.AbstractConnection.DataReceiver;
import de.uniduesseldorf.dxram.core.recovery.RecoveryMessages;
import de.uniduesseldorf.dxram.utils.StatisticsManager;

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

	private ConnectionManager m_manager;

	// Constructors
	/**
	 * Creates an instance of NetworkHandler
	 */
	public NetworkHandler() {
		m_executor = TaskExecutor.getDefaultExecutor();
		m_receivers = new HashMap<>();
		m_messageHandler = new MessageHandler();
	}

	// Methods
	@Override
	public synchronized void initialize() throws NetworkException {
		final byte networkType;
		final byte chunkType;
		final byte logType;
		final byte lookupType;
		final byte recoveryType;

		LOGGER.trace("Entering initialize");

		// Network Messages
		networkType = NIOConnectionCreator.FlowControlMessage.TYPE;
		MessageDirectory.register(networkType, NIOConnectionCreator.FlowControlMessage.SUBTYPE, NIOConnectionCreator.FlowControlMessage.class);

		// Chunk Messages
		chunkType = ChunkMessages.TYPE;
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_GET_REQUEST, ChunkMessages.GetRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_GET_RESPONSE, ChunkMessages.GetResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_PUT_REQUEST, ChunkMessages.PutRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_PUT_RESPONSE, ChunkMessages.PutResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_REMOVE_REQUEST, ChunkMessages.RemoveRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_REMOVE_RESPONSE, ChunkMessages.RemoveResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_LOCK_REQUEST, ChunkMessages.LockRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_LOCK_RESPONSE, ChunkMessages.LockResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_UNLOCK_MESSAGE, ChunkMessages.UnlockMessage.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_DATA_REQUEST, ChunkMessages.DataRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_DATA_RESPONSE, ChunkMessages.DataResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_DATA_MESSAGE, ChunkMessages.DataMessage.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_MULTIGET_REQUEST, ChunkMessages.MultiGetRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_MULTIGET_RESPONSE, ChunkMessages.MultiGetResponse.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_CHUNK_COMMAND_MESSAGE, ChunkMessages.ChunkCommandMessage.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_CHUNK_COMMAND_REQUEST, ChunkMessages.ChunkCommandRequest.class);
		MessageDirectory.register(chunkType, ChunkMessages.SUBTYPE_CHUNK_COMMAND_RESPONSE, ChunkMessages.ChunkCommandResponse.class);

		// Log Messages
		logType = LogMessages.TYPE;
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_REQUEST, LogMessages.LogRequest.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_RESPONSE, LogMessages.LogResponse.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_MESSAGE, LogMessages.LogMessage.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_REMOVE_MESSAGE, LogMessages.RemoveMessage.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_INIT_REQUEST, LogMessages.InitRequest.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_INIT_RESPONSE, LogMessages.InitResponse.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_COMMAND_REQUEST, LogMessages.LogCommandRequest.class);
		MessageDirectory.register(logType, LogMessages.SUBTYPE_LOG_COMMAND_RESPONSE, LogMessages.LogCommandResponse.class);

		// Lookup Messages
		lookupType = LookupMessages.TYPE;
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_JOIN_REQUEST, LookupMessages.JoinRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_JOIN_RESPONSE, LookupMessages.JoinResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_REQUEST, LookupMessages.InitRangeRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INIT_RANGE_RESPONSE, LookupMessages.InitRangeResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_REQUEST, LookupMessages.LookupRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_RESPONSE, LookupMessages.LookupResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_REQUEST, LookupMessages.GetBackupRangesRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_BACKUP_RANGES_RESPONSE, LookupMessages.GetBackupRangesResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_UPDATE_ALL_MESSAGE, LookupMessages.UpdateAllMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_REQUEST, LookupMessages.MigrateRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_RESPONSE, LookupMessages.MigrateResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_MESSAGE, LookupMessages.MigrateMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_REQUEST, LookupMessages.MigrateRangeRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_MIGRATE_RANGE_RESPONSE, LookupMessages.MigrateRangeResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_REMOVE_REQUEST, LookupMessages.RemoveRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_REMOVE_RESPONSE, LookupMessages.RemoveResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEND_BACKUPS_MESSAGE, LookupMessages.SendBackupsMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEND_SUPERPEERS_MESSAGE, LookupMessages.SendSuperpeersMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_REQUEST, LookupMessages.AskAboutBackupsRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_BACKUPS_RESPONSE, LookupMessages.AskAboutBackupsResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_REQUEST, LookupMessages.AskAboutSuccessorRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_ASK_ABOUT_SUCCESSOR_RESPONSE, LookupMessages.AskAboutSuccessorResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_PREDECESSOR_MESSAGE,
				LookupMessages.NotifyAboutNewPredecessorMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_NEW_SUCCESSOR_MESSAGE, LookupMessages.NotifyAboutNewSuccessorMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_PING_SUPERPEER_MESSAGE, LookupMessages.PingSuperpeerMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_REQUEST, LookupMessages.SearchForPeerRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_SEARCH_FOR_PEER_RESPONSE, LookupMessages.SearchForPeerResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_REQUEST, LookupMessages.PromotePeerRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_PROMOTE_PEER_RESPONSE, LookupMessages.PromotePeerResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_DELEGATE_PROMOTE_PEER_MESSAGE, LookupMessages.DelegatePromotePeerMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_NOTIFY_ABOUT_FAILED_PEER_MESSAGE, LookupMessages.NotifyAboutFailedPeerMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_START_RECOVERY_MESSAGE, LookupMessages.StartRecoveryMessage.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INSERT_ID_REQUEST, LookupMessages.InsertIDRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_INSERT_ID_RESPONSE, LookupMessages.InsertIDResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_REQUEST, LookupMessages.GetChunkIDRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_CHUNKID_RESPONSE, LookupMessages.GetChunkIDResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_REQUEST, LookupMessages.GetMappingCountRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_GET_MAPPING_COUNT_RESPONSE, LookupMessages.GetMappingCountResponse.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_REQUEST, LookupMessages.LookupReflectionRequest.class);
		MessageDirectory.register(lookupType, LookupMessages.SUBTYPE_LOOKUP_REFLECTION_RESPONSE, LookupMessages.LookupReflectionResponse.class);

		// Recovery Messages
		recoveryType = RecoveryMessages.TYPE;
		MessageDirectory.register(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_MESSAGE, RecoveryMessages.RecoverMessage.class);
		MessageDirectory.register(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_REQUEST, RecoveryMessages.RecoverBackupRangeRequest.class);
		MessageDirectory.register(recoveryType, RecoveryMessages.SUBTYPE_RECOVER_BACKUP_RANGE_RESPONSE, RecoveryMessages.RecoverBackupRangeResponse.class);

		m_manager = new ConnectionManager(AbstractConnectionCreator.getInstance(), this);

		if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_THROUGHPUT)) {
			StatisticsManager.registerStatistic("Throughput", ThroughputStatistic.getInstance());
		}
		if (Core.getConfiguration().getBooleanValue(ConfigurationConstants.STATISTIC_REQUEST)) {
			StatisticsManager.registerStatistic("Request", RequestStatistic.getInstance());
		}

		LOGGER.trace("Exiting initialize");
	}

	@Override
	public synchronized void activateConnectionManager() {
		m_manager.activate();
	}

	@Override
	public synchronized void deactivateConnectionManager() {
		m_manager.deactivate();
	}

	@Override
	public synchronized void close() {
		LOGGER.trace("Entering close");

		m_executor.shutdown();
		m_messageHandler.m_executor.shutdown();

		LOGGER.trace("Exiting close");
	}

	@Override
	public void register(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			synchronized (m_receivers) {
				entry = m_receivers.get(p_message);
				if (entry == null) {
					entry = new Entry();
					m_receivers.put(p_message, entry);
				}
				entry.add(p_receiver);

				LOGGER.info("new MessageReceiver");
			}
		}
	}

	@Override
	public void unregister(final Class<? extends AbstractMessage> p_message, final MessageReceiver p_receiver) {
		Entry entry;

		if (p_receiver != null) {
			synchronized (m_receivers) {
				entry = m_receivers.get(p_message);
				if (entry != null) {
					entry.remove(p_receiver);

					LOGGER.info("MessageReceiver removed");
				}
			}
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
			if (p_message.getDestination() == NodeID.getLocalNodeID()) {
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
		private final ArrayDeque<AbstractMessage> m_messages;
		private final TaskExecutor m_executor;

		// Constructors
		/**
		 * Creates an instance of MessageHandler
		 */
		public MessageHandler() {
			m_executor = new TaskExecutor("MessageHandler", Core.getConfiguration().getIntValue(ConfigurationConstants.NETWORK_MESSAGE_HANDLER_THREAD_COUNT));
			m_messages = new ArrayDeque<>();
		}

		// Methods
		/**
		 * Enqueue a new message for delivering
		 * @param p_message
		 *            the message
		 */
		public void newMessage(final AbstractMessage p_message) {
			synchronized (m_messages) {
				m_messages.offer(p_message);
			}

			m_executor.execute(this);
		}

		@Override
		public void run() {
			AbstractMessage message;
			Entry entry;

			synchronized (m_messages) {
				message = m_messages.poll();
			}

			synchronized (m_receivers) {
				entry = m_receivers.get(message.getClass());
			}

			if (entry != null) {
				entry.newMessage(message);
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
		public Entry() {
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
