package de.hhu.bsinfo.net;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractMessage;

/**
 * Executes incoming exclusive messages
 *
 * @author Kevin Beineke 19.07.2016
 */
class ExclusiveMessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ExclusiveMessageHandler.class.getSimpleName());

    private static final int EXCLUSIVE_MESSAGE_STORE_SIZE = 20;

    private final MessageReceiverStore m_messageReceivers;
    private final MessageStore m_exclusiveMessages;
    private final ReentrantLock m_exclusiveMessagesLock;
    private volatile boolean m_shutdown;

    /**
     * Creates an instance of MessageHandler
     */
    ExclusiveMessageHandler(final MessageReceiverStore p_messageReceivers) {
        m_messageReceivers = p_messageReceivers;
        m_exclusiveMessages = new MessageStore(EXCLUSIVE_MESSAGE_STORE_SIZE);
        m_exclusiveMessagesLock = new ReentrantLock(false);
    }

    /**
     * Closes the handler
     */
    public void shutdown() {
        m_shutdown = true;
    }

    @Override
    public void run() {
        long time;
        AbstractMessage message = null;
        MessageReceiver messageReceiver;

        while (!m_shutdown) {
            while (message == null && !m_shutdown) {
                m_exclusiveMessagesLock.lock();
                if (m_exclusiveMessages.isEmpty()) {
                    m_exclusiveMessagesLock.unlock();
                    LockSupport.park();
                    m_exclusiveMessagesLock.lock();
                }

                message = m_exclusiveMessages.popMessage();
                m_exclusiveMessagesLock.unlock();
            }

            if (m_shutdown) {
                break;
            }

            messageReceiver = m_messageReceivers.getReceiver(message.getType(), message.getSubtype());

            if (messageReceiver != null) {
                messageReceiver.onIncomingMessage(message);
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("No message receiver was registered for %d, %d", message.getType(), message.getSubtype());
                // #endif /* LOGGER >= ERROR */
            }
            message = null;
        }
    }

    /**
     * Enqueue a new message for delivering
     *
     * @param p_message
     *         the message
     */
    void newMessage(final AbstractMessage p_message) {
        boolean wakeup = false;

        m_exclusiveMessagesLock.lock();
        if (m_exclusiveMessages.isEmpty()) {
            wakeup = true;
        }

        while (!m_exclusiveMessages.pushMessage(p_message)) {
            m_exclusiveMessagesLock.unlock();
            LockSupport.unpark(this);
            Thread.yield();
            m_exclusiveMessagesLock.lock();
        }
        m_exclusiveMessagesLock.unlock();

        if (wakeup) {
            LockSupport.unpark(this);
        }
    }

    void newMessages(final AbstractMessage[] p_messages) {
        boolean wakeup = false;

        m_exclusiveMessagesLock.lock();
        if (m_exclusiveMessages.isEmpty()) {
            wakeup = true;
        }

        for (AbstractMessage message : p_messages) {
            if (message == null) {
                break;
            }

            while (!m_exclusiveMessages.pushMessage(message)) {
                m_exclusiveMessagesLock.unlock();
                LockSupport.unpark(this);
                Thread.yield();
                m_exclusiveMessagesLock.lock();
            }
        }
        m_exclusiveMessagesLock.unlock();

        if (wakeup) {
            LockSupport.unpark(this);
        }
    }
}
