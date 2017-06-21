package de.hhu.bsinfo.net;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractMessage;

/**
 * Distributes incoming default messages
 *
 * @author Kevin Beineke 19.07.2016
 */
final class DefaultMessageHandlerPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandlerPool.class.getSimpleName());

    private static final int DEFAULT_MESSAGE_STORE_SIZE = 100;

    private final MessageStore m_defaultMessages;

    private final DefaultMessageHandler[] m_threads;
    private final ReentrantLock m_defaultMessagesLock;

    /**
     * Creates an instance of DefaultMessageHandlerPool
     *
     * @param p_numMessageHandlerThreads
     *         the number of default message handler
     */
    DefaultMessageHandlerPool(final MessageReceiverStore p_messageReceivers, final int p_numMessageHandlerThreads) {
        m_defaultMessages = new MessageStore(DEFAULT_MESSAGE_STORE_SIZE);
        m_defaultMessagesLock = new ReentrantLock(false);

        // #if LOGGER >= INFO
        LOGGER.info("Network: DefaultMessageHandlerPool: Initialising %d threads", p_numMessageHandlerThreads);
        // #endif /* LOGGER >= INFO */

        DefaultMessageHandler t;
        m_threads = new DefaultMessageHandler[p_numMessageHandlerThreads];
        for (int i = 0; i < m_threads.length; i++) {
            t = new DefaultMessageHandler(p_messageReceivers, m_defaultMessages, m_defaultMessagesLock);
            t.setName("Network: DefaultMessageHandler " + (i + 1));
            m_threads[i] = t;
            t.start();
        }
    }

    /**
     * Closes all default message handler
     */
    void shutdown() {
        DefaultMessageHandler t;
        for (int i = 0; i < m_threads.length; i++) {
            t = m_threads[i];
            t.shutdown();
            LockSupport.unpark(t);
            t.interrupt();

            try {
                t.join();
                // #if LOGGER >= INFO
                LOGGER.info("Shutdown of DefaultMessageHandler %d successful", i + 1);
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
     * @param p_message
     *         the message
     */
    void newMessage(final AbstractMessage p_message) {
        boolean wakeup = false;

        // Ignore network test messages (e.g. ping after response delay)
        if (p_message.getType() != 0 || p_message.getSubtype() != 0) {
            m_defaultMessagesLock.lock();
            if (m_defaultMessages.isEmpty()) {
                wakeup = true;
            }

            while (!m_defaultMessages.pushMessage(p_message)) {
                m_defaultMessagesLock.unlock();
                for (Thread thread : m_threads) {
                    LockSupport.unpark(thread);
                }
                Thread.yield();
                m_defaultMessagesLock.lock();
            }
            m_defaultMessagesLock.unlock();
            if (wakeup) {
                for (Thread thread : m_threads) {
                    LockSupport.unpark(thread);
                }
            }
        }
    }

    /**
     * Enqueues a batch of new messages for delivering
     *
     * @param p_messages
     *         the messages
     */
    void newMessages(final AbstractMessage[] p_messages) {
        boolean wakeup = false;

        m_defaultMessagesLock.lock();
        if (m_defaultMessages.isEmpty()) {
            wakeup = true;
        }

        for (AbstractMessage message : p_messages) {
            if (message == null) {
                break;
            }

            // Ignore network test messages (e.g. ping after response delay)
            if (message.getType() != 0 || message.getSubtype() != 0) {
                while (!m_defaultMessages.pushMessage(message)) {
                    m_defaultMessagesLock.unlock();
                    for (Thread thread : m_threads) {
                        LockSupport.unpark(thread);
                    }
                    Thread.yield();
                    m_defaultMessagesLock.lock();
                }
            }
        }
        m_defaultMessagesLock.unlock();

        if (wakeup) {
            for (Thread thread : m_threads) {
                LockSupport.unpark(thread);
            }
        }
    }
}
