package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * Executes incoming exclusive messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.07.2016
 */
class ExclusiveMessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(ExclusiveMessageHandler.class.getSimpleName());
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Pop");
    private static final StatisticsOperation SOP_POP_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Wait");
    private static final StatisticsOperation SOP_EXECUTE = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Execute");
    private static final StatisticsOperation SOP_PUSH = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Push");
    private static final StatisticsOperation SOP_PUSH_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandlerPool.class, "Wait");

    private static final int EXCLUSIVE_MESSAGE_STORE_SIZE = 20;

    private final MessageReceiverStore m_messageReceivers;
    private final MessageStore m_exclusiveMessages;
    private volatile boolean m_shutdown;

    /**
     * Creates an instance of MessageHandler
     */
    ExclusiveMessageHandler(final MessageReceiverStore p_messageReceivers) {
        m_messageReceivers = p_messageReceivers;
        m_exclusiveMessages = new MessageStore(EXCLUSIVE_MESSAGE_STORE_SIZE);
    }

    /**
     * Closes the handler
     */
    public void shutdown() {
        m_shutdown = true;
    }

    @Override
    public void run() {
        int waitCounter = 0;
        Message message = null;
        MessageReceiver messageReceiver;

        while (!m_shutdown) {
            // #ifdef STATISTICS
            SOP_POP.enter();
            // #endif /* STATISTICS */

            while (message == null && !m_shutdown) {
                if (m_exclusiveMessages.isEmpty()) {
                    // #ifdef STATISTICS
                    SOP_POP_WAIT.enter();
                    // #endif /* STATISTICS */

                    if (waitCounter++ <= 10000) {
                        // No new message at the moment -> sleep for xx µs and try again
                        LockSupport.parkNanos(1);
                    } else {
                        // No new message for a longer period -> increase sleep to 1 ms to reduce cpu load
                        LockSupport.parkNanos(1000 * 1000);
                    }

                    // #ifdef STATISTICS
                    SOP_POP_WAIT.leave();
                    // #endif /* STATISTICS */
                } else {
                    waitCounter = 0;
                }

                message = m_exclusiveMessages.popMessage();
            }

            // #ifdef STATISTICS
            SOP_POP.leave();
            // #endif /* STATISTICS */

            if (m_shutdown) {
                break;
            }

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
                LOGGER.error("No message receiver was registered for %d, %d", message.getType(), message.getSubtype());
                // #endif /* LOGGER >= ERROR */
            }
            message = null;
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

            while (!m_exclusiveMessages.pushMessage(message)) {
                // #ifdef STATISTICS
                SOP_PUSH_WAIT.enter();
                // #endif /* STATISTICS */

                Thread.yield();

                // #ifdef STATISTICS
                SOP_PUSH_WAIT.leave();
                // #endif /* STATISTICS */
            }
        }

        // #ifdef STATISTICS
        SOP_PUSH.leave();
        // #endif /* STATISTICS */
    }
}
