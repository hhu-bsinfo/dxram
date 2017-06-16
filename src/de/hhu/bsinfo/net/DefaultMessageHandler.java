package de.hhu.bsinfo.net;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractMessage;

/**
 * Executes incoming default messages
 *
 * @author Kevin Beineke 19.07.2016
 */
final class DefaultMessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandler.class.getSimpleName());

    // Attributes
    private final MessageReceiverStore m_messageReceivers;
    private final MessageStore m_defaultMessages;
    private final ReentrantLock m_defaultMessagesLock;
    private volatile boolean m_shutdown;

    // Constructors

    /**
     * Creates an instance of DefaultMessageHandler
     *
     * @param p_queue
     *         the message queue
     * @param p_lock
     *         the lock for accessing message queue
     */
    DefaultMessageHandler(final MessageReceiverStore p_messageReceivers, final MessageStore p_queue, final ReentrantLock p_lock) {
        m_messageReceivers = p_messageReceivers;
        m_defaultMessages = p_queue;
        m_defaultMessagesLock = p_lock;
    }

    /**
     * Closes the handler
     */
    public void shutdown() {
        m_shutdown = true;
    }

    // Methods
    @Override
    public void run() {
        AbstractMessage message = null;
        MessageReceiver messageReceiver;

        while (!m_shutdown) {
            while (message == null && !m_shutdown) {
                m_defaultMessagesLock.lock();
                if (m_defaultMessages.isEmpty()) {
                    m_defaultMessagesLock.unlock();
                    LockSupport.park();
                    m_defaultMessagesLock.lock();
                }

                message = m_defaultMessages.popMessage();
                m_defaultMessagesLock.unlock();
            }

            if (m_shutdown) {
                break;
            }

            messageReceiver = m_messageReceivers.getReceiver(message.getType(), message.getSubtype());

            if (messageReceiver != null) {
                messageReceiver.onIncomingMessage(message);
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("No message receiver was registered for %d, %d!", message.getType(), message.getSubtype());
                // #endif /* LOGGER >= ERROR */
            }
            message = null;
        }
    }
}
