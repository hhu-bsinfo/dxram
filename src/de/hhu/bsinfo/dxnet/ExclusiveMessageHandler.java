package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.Messages;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * Distributes incoming exclusive messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 30.08.2017
 */
final class ExclusiveMessageHandler {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ExclusiveMessageHandler.class.getSimpleName());
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(ExclusiveMessageHandler.class, "Push");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(ExclusiveMessageHandler.class, "Wait");

    // must be a power of two to work with wrap around
    private static final int EXCLUSIVE_MESSAGE_STORE_SIZE = 32;

    private final MessageStore m_exclusiveMessages;

    private final MessageHandler m_exclusiveMessageHandler;

    /**
     * Creates an instance of ExlusiveMessageHandler
     *
     * @param p_messageReceivers
     *         Provides all registered message receivers
     */
    ExclusiveMessageHandler(final MessageReceiverStore p_messageReceivers) {
        m_exclusiveMessages = new MessageStore(EXCLUSIVE_MESSAGE_STORE_SIZE);

        // #if LOGGER >= INFO
        LOGGER.info("Network: ExclusiveMessageHandler: Initialising thread");
        // #endif /* LOGGER >= INFO */

        m_exclusiveMessageHandler = new MessageHandler(p_messageReceivers, m_exclusiveMessages);
        m_exclusiveMessageHandler.setName("Network: ExclusiveMessageHandler");
        m_exclusiveMessageHandler.start();
    }

    /**
     * Closes the exclusive message handler
     */
    void shutdown() {
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
     * Enqueues a batch of new exclusive messages for delivering
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
                while (!m_exclusiveMessages.pushMessage(message)) {
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
