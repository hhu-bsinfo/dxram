package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;
import de.hhu.bsinfo.dxnet.core.Message;

/**
 * Executes incoming default messages
 *
 * @author Kevin Beineke 19.07.2016
 */
final class DefaultMessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandler.class.getSimpleName());
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Pop");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Wait");
    private static final StatisticsOperation SOP_EXECUTE = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Execute");

    // Attributes
    private final int m_id;
    private final MessageReceiverStore m_messageReceivers;
    private final MessageStore m_defaultMessages;
    private final DefaultMessageHandlerPool m_pool;
    private volatile boolean m_shutdown;

    // Constructors

    /**
     * Creates an instance of DefaultMessageHandler
     *
     * @param p_queue
     *         the message queue
     */
    DefaultMessageHandler(final int p_id, final MessageReceiverStore p_messageReceivers, final MessageStore p_queue, final DefaultMessageHandlerPool p_pool) {
        m_id = p_id;
        m_messageReceivers = p_messageReceivers;
        m_defaultMessages = p_queue;
        m_pool = p_pool;
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
        int messagesLeft = 0;
        int capacityThreshold = (int) (m_defaultMessages.capacity() * 0.9);
        int waitCounter = 0;
        Message message = null;
        MessageReceiver messageReceiver;

        while (!m_shutdown) {

            // #ifdef STATISTICS
            SOP_POP.enter();
            // #endif /* STATISTICS */

            while (message == null && !m_shutdown) {
                if (m_defaultMessages.isEmpty()) {
                    // #ifdef STATISTICS
                    SOP_WAIT.enter();
                    // #endif /* STATISTICS */

                    if (m_id == 0) {
                        // First default message handler is always available
                        if (waitCounter++ <= 10000) {
                            // No new message at the moment -> sleep for xx µs and try again
                            LockSupport.parkNanos(1);
                        } else {
                            // No new message for a longer period -> increase sleep to 1 ms to reduce cpu load
                            LockSupport.parkNanos(1000 * 1000);
                        }
                    } else {
                        // Other default message handlers idle until first message handler calls for support (see below)
                        LockSupport.park();
                    }

                    // #ifdef STATISTICS
                    SOP_WAIT.leave();
                    // #endif /* STATISTICS */
                } else {
                    waitCounter = 0;
                }

                message = m_defaultMessages.popMessage();
                messagesLeft = m_defaultMessages.size();
            }

            if (m_shutdown) {
                break;
            }

            if (m_id == 0 && messagesLeft > capacityThreshold) {
                // Calling for support
                m_pool.wakeupMessageHandlers();
            }

            // #ifdef STATISTICS
            SOP_POP.leave();
            // #endif /* STATISTICS */

            messageReceiver = m_messageReceivers.getReceiver(message.getType(), message.getSubtype());

            if (messageReceiver != null) {
                // #ifdef STATISTICS
                SOP_EXECUTE.enter();
                // #endif /* STATISTICS */

                messageReceiver.onIncomingMessage(message);

                // #ifdef STATISTICS
                SOP_EXECUTE.leave();
                // #endif /* STATISTICS */
            } else {
                // #if LOGGER >= ERROR
                LOGGER.error("No message receiver was registered for %d, %d!", message.getType(), message.getSubtype());
                // #endif /* LOGGER >= ERROR */
            }
            message = null;
        }
    }
}
