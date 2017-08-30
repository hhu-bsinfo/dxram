package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.Messages;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * Distributes incoming default messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.07.2016
 */
final class DefaultMessageHandlerPool {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandlerPool.class.getSimpleName());
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Push");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Wait");

    // must be a power of two to work with wrap around
    private static final int SIZE_MESSAGE_STORE = 16 * 1024;

    private final MessageStore m_defaultMessages;

    private final MessageHandler[] m_threads;

    /**
     * Creates an instance of DefaultMessageHandlerPool
     *
     * @param p_numMessageHandlerThreads
     *         the number of default message handler
     */
    DefaultMessageHandlerPool(final MessageReceiverStore p_messageReceivers, final int p_numMessageHandlerThreads) {
        m_defaultMessages = new MessageStore(SIZE_MESSAGE_STORE);

        // #if LOGGER >= INFO
        LOGGER.info("Network: DefaultMessageHandlerPool: Initialising %d threads", p_numMessageHandlerThreads);
        // #endif /* LOGGER >= INFO */

        MessageHandler t;
        m_threads = new MessageHandler[p_numMessageHandlerThreads];
        for (int i = 0; i < m_threads.length; i++) {
            t = new MessageHandler(p_messageReceivers, m_defaultMessages);
            t.setName("Network: MessageHandler " + (i + 1));
            m_threads[i] = t;
            t.start();
        }
    }

    /**
     * Closes all default message handler
     */
    void shutdown() {
        MessageHandler t;
        for (int i = 0; i < m_threads.length; i++) {
            t = m_threads[i];
            t.shutdown();
            LockSupport.unpark(t);
            t.interrupt();

            try {
                t.join();
                // #if LOGGER >= INFO
                LOGGER.info("Shutdown of MessageHandler %d successful", i + 1);
                // #endif /* LOGGER >= INFO */
            } catch (final InterruptedException e) {
                // #if LOGGER >= WARN
                LOGGER.warn("Could not wait for default message handler to finish. Interrupted");
                // #endif /* LOGGER >= WARN */
            }
        }
    }

    /**
     * Enqueues a batch of new messages for delivering
     *
     * @param p_messages
     *         the messages
     */
    void newMessages(final Message[] p_messages) {
        // #ifdef STATISTICS
        SOP_PUSH.enter();
        // #endif /* STATISTICS */

        for (Message message : p_messages) {
            if (message == null) {
                break;
            }

            // Ignore network test messages (e.g. ping after response delay)
            if (!(message.getType() == Messages.NETWORK_MESSAGES_TYPE && message.getSubtype() == Messages.SUBTYPE_DEFAULT_MESSAGE)) {
                while (!m_defaultMessages.pushMessage(message)) {
                    // #ifdef STATISTICS
                    SOP_WAIT.enter();
                    // #endif /* STATISTICS */

                    Thread.yield();

                    // #ifdef STATISTICS
                    SOP_WAIT.leave();
                    // #endif /* STATISTICS */
                }
            }
        }

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */
    }
}
