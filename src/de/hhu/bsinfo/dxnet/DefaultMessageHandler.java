package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.stats.StatisticsOperation;
import de.hhu.bsinfo.dxram.stats.StatisticsRecorderManager;

/**
 * Executes incoming default messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.07.2016
 */
final class DefaultMessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(DefaultMessageHandler.class.getSimpleName());
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Pop");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Wait");
    private static final StatisticsOperation SOP_SLEEP = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Sleep");
    private static final StatisticsOperation SOP_EXECUTE = StatisticsRecorderManager.getOperation(DefaultMessageHandler.class, "Execute");

    private static final int THRESHOLD_PARK = 100000;
    private static final int THRESHOLD_PARK_SLEEP = 1000;

    private final MessageReceiverStore m_messageReceivers;
    private final MessageStore m_defaultMessages;
    private volatile boolean m_shutdown;

    // Constructors

    /**
     * Creates an instance of DefaultMessageHandler
     *
     * @param p_queue
     *         the message queue
     */
    DefaultMessageHandler(final MessageReceiverStore p_messageReceivers, final MessageStore p_queue) {
        m_messageReceivers = p_messageReceivers;
        m_defaultMessages = p_queue;
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
        int waitCounter = 0;
        int sleepCounter = 0;
        Message message;
        MessageReceiver messageReceiver;

        while (!m_shutdown) {
            // #ifdef STATISTICS
            SOP_POP.enter();
            // #endif /* STATISTICS */

            message = m_defaultMessages.popMessage();

            if (message == null) {
                // keep latency low (especially on infiniband) but also keep cpu load low
                // avoid parking on every iteration -> increases overall latency for messages
                if (sleepCounter > THRESHOLD_PARK_SLEEP) {
                    // #ifdef STATISTICS
                    SOP_WAIT.enter();
                    // #endif /* STATISTICS */

                    // No new message for a longer period -> increase sleep to 1 ms to reduce cpu load
                    // continue sleeping until new messages are available
                    LockSupport.parkNanos(1000 * 1000);

                    // #ifdef STATISTICS
                    SOP_WAIT.leave();
                    // #endif /* STATISTICS */
                } else if (waitCounter > THRESHOLD_PARK) {
                    // #ifdef STATISTICS
                    SOP_SLEEP.enter();
                    // #endif /* STATISTICS */

                    // No new message at the moment -> sleep for xx µs and try again
                    LockSupport.parkNanos(1);
                    waitCounter = 0;
                    sleepCounter++;

                    // #ifdef STATISTICS
                    SOP_SLEEP.leave();
                    // #endif /* STATISTICS */
                } else {
                    waitCounter++;
                }

                // #ifdef STATISTICS
                SOP_POP.leave();
                // #endif /* STATISTICS */

                continue;
            }

            // #ifdef STATISTICS
            SOP_POP.leave();
            // #endif /* STATISTICS */

            // reset waits and sleeps
            waitCounter = 0;
            sleepCounter = 0;

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
        }
    }
}
