package de.hhu.bsinfo.net;

import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.Messages;

/**
 * Distributes incoming default messages
 *
 * @author Kevin Beineke 19.07.2016
 */
final class DefaultMessageHandlerPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandlerPool.class.getSimpleName());
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Push");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Wait");

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
            t = new DefaultMessageHandler(i, p_messageReceivers, m_defaultMessages, m_defaultMessagesLock, this);
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
     * Wake-up default message handlers. Is called by first message handler to get support.
     */
    void wakeupMessageHandlers() {
        for (int i = 1; i < m_threads.length; i++) {
            LockSupport.unpark(m_threads[i]);
        }
    }

    /**
     * Enqueue a new message for delivering
     *
     * @param p_message
     *         the message
     */
    void newMessage(final AbstractMessage p_message) {
        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        // Ignore network test messages (e.g. ping after response delay)
        if (!(p_message.getType() == Messages.NETWORK_MESSAGES_TYPE && p_message.getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE)) {
            m_defaultMessagesLock.lock();
            while (!m_defaultMessages.pushMessage(p_message)) {
                m_defaultMessagesLock.unlock();

                // #ifdef STATISTICS
                SOP_WAIT.enter();
                // #endif /* STATISTICS */

                Thread.yield();

                // #ifdef STATISTICS
                SOP_WAIT.leave();
                // #endif /* STATISTICS */

                m_defaultMessagesLock.lock();
            }
            m_defaultMessagesLock.unlock();
        }

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */
    }

    /**
     * Enqueues a batch of new messages for delivering
     *
     * @param p_messages
     *         the messages
     */
    void newMessages(final AbstractMessage[] p_messages) {
        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        m_defaultMessagesLock.lock();
        for (AbstractMessage message : p_messages) {
            if (message == null) {
                break;
            }

            // Ignore network test messages (e.g. ping after response delay)
            if (!(message.getType() == Messages.NETWORK_MESSAGES_TYPE && message.getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE)) {
                while (!m_defaultMessages.pushMessage(message)) {
                    m_defaultMessagesLock.unlock();

                    // #ifdef STATISTICS
                    SOP_WAIT.enter();
                    // #endif /* STATISTICS */

                    Thread.yield();

                    // #ifdef STATISTICS
                    SOP_WAIT.leave();
                    // #endif /* STATISTICS */

                    m_defaultMessagesLock.lock();
                }
            }
        }
        m_defaultMessagesLock.unlock();

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */
    }
}
