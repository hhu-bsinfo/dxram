package de.hhu.bsinfo.net;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.DataReceiver;

/**
 * Created by nothaas on 6/12/17.
 */
final class MessageHandlers implements DataReceiver, MessageReceiverStore {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandlers.class.getSimpleName());

    private MessageReceiver[][] m_receivers;
    private final ReentrantLock m_receiversLock;

    private final DefaultMessageHandlerPool m_defaultMessageHandlerPool;
    private final ExclusiveMessageHandler m_exclusiveMessageHandler;

    private final int m_requestTimeOut;

    MessageHandlers(final int p_numMessageHandlerThreads, final int p_requestTimeOut) {
        m_requestTimeOut = p_requestTimeOut;

        m_receivers = new MessageReceiver[100][100];
        m_receiversLock = new ReentrantLock(false);

        m_defaultMessageHandlerPool = new DefaultMessageHandlerPool(this, p_numMessageHandlerThreads);

        m_exclusiveMessageHandler = new ExclusiveMessageHandler(this);
        m_exclusiveMessageHandler.setName("Network: ExclusiveMessageHandler");
        m_exclusiveMessageHandler.start();
    }

    /**
     * Registers a message receiver
     *
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_receiver
     *         the receiver
     */
    void register(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        if (p_receiver != null) {
            m_receiversLock.lock();
            // enlarge array
            if (m_receivers.length <= p_type) {
                final MessageReceiver[][] newArray = new MessageReceiver[p_type + 1][];
                System.arraycopy(m_receivers, 0, newArray, 0, m_receivers.length);
                m_receivers = newArray;
            }

            // create new sub array when it is not existing until now
            if (m_receivers[p_type] == null) {
                m_receivers[p_type] = new MessageReceiver[p_subtype + 1];
            }

            // enlarge subtype array
            if (m_receivers[p_type].length <= p_subtype) {
                final MessageReceiver[] newArray = new MessageReceiver[p_subtype + 1];
                System.arraycopy(m_receivers[p_type], 0, newArray, 0, m_receivers[p_type].length);
                m_receivers[p_type] = newArray;
            }

            if (m_receivers[p_type][p_subtype] != null) {
                // #if LOGGER >= WARN
                LOGGER.warn("Receiver for %d %d is already registered", p_type, p_subtype);
                // #endif /* LOGGER >= WARN */
            }
            m_receivers[p_type][p_subtype] = p_receiver;

            // #if LOGGER >= TRACE
            LOGGER.trace("Added new MessageReceiver %s for %d %d", p_receiver.getClass(), p_type, p_subtype);
            // #endif /* LOGGER >= TRACE */
            m_receiversLock.unlock();
        }
    }

    /**
     * Unregisters a message receiver
     *
     * @param p_type
     *         the message type
     * @param p_subtype
     *         the message subtype
     * @param p_receiver
     *         the receiver
     */
    void unregister(final byte p_type, final byte p_subtype, final MessageReceiver p_receiver) {
        if (p_receiver != null) {
            m_receiversLock.lock();
            m_receivers[p_type][p_subtype] = null;
            m_receiversLock.unlock();
        }
    }

    void close() {
        // Shutdown default message handler(s)
        m_defaultMessageHandlerPool.shutdown();

        // Shutdown exclusive message handler
        m_exclusiveMessageHandler.shutdown();
        LockSupport.unpark(m_exclusiveMessageHandler);
        m_exclusiveMessageHandler.interrupt();
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
    }

    /**
     * Handles an incoming Message
     *
     * @param p_message
     *         the incoming Message
     */
    @Override
    public void newMessage(final AbstractMessage p_message) {
        // #if LOGGER == TRACE
        LOGGER.trace("Received new message: %s", p_message);
        // #endif /* LOGGER == TRACE */

        if (!p_message.isExclusive()) {
            m_defaultMessageHandlerPool.newMessage(p_message);
        } else {
            m_exclusiveMessageHandler.newMessage(p_message);
        }
    }

    @Override
    public void newMessages(final AbstractMessage[] p_messages) {
        // #if LOGGER == TRACE
        LOGGER.trace("Received new messages");
        // #endif /* LOGGER == TRACE */

        if (!p_messages[0].isExclusive()) {
            m_defaultMessageHandlerPool.newMessages(p_messages);
        } else {
            m_exclusiveMessageHandler.newMessages(p_messages);
        }
    }

    @Override
    public MessageReceiver getReceiver(final byte p_type, final byte p_subtype) {
        long deadline;
        MessageReceiver messageReceiver = m_receivers[p_type][p_subtype];

        // Try again in a loop, if receivers were not registered. Stop if request timeout is reached as answering later has no effect
        if (messageReceiver == null) {
            // #if LOGGER >= WARN
            LOGGER.warn("Message receiver null for %d, %d! Waiting...", p_type, p_subtype);
            // #endif /* LOGGER >= WARN */
            deadline = System.currentTimeMillis() + m_requestTimeOut;
            while (messageReceiver == null && System.currentTimeMillis() < deadline) {
                m_receiversLock.lock();
                messageReceiver = m_receivers[p_type][p_subtype];
                m_receiversLock.unlock();
            }
        }

        return messageReceiver;
    }
}
