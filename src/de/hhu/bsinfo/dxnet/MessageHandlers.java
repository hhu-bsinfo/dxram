package de.hhu.bsinfo.dxnet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;

/**
 * Provides message handlers for incoming messages
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 12.06.2017
 */
public final class MessageHandlers {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandlers.class.getSimpleName());

    private final DefaultMessageHandlerPool m_defaultMessageHandlerPool;
    private final ExclusiveMessageHandler m_exclusiveMessageHandler;

    /**
     * Constructor
     *
     * @param p_numMessageHandlerThreads
     *         Number of message handler threads to run
     * @param p_messageReceivers
     *         Provides all registered message receivers
     */
    MessageHandlers(final int p_numMessageHandlerThreads, final MessageReceiverStore p_messageReceivers) {
        // default message handlers
        m_defaultMessageHandlerPool = new DefaultMessageHandlerPool(p_messageReceivers, p_numMessageHandlerThreads);

        // and one exclusive
        m_exclusiveMessageHandler = new ExclusiveMessageHandler(p_messageReceivers);
    }

    /**
     * Called when new messages arrived
     *
     * @param p_messages
     *         Array with new incoming messages to handle
     */
    public void newMessages(final Message[] p_messages) {
        // #if LOGGER == TRACE
        LOGGER.trace("Received new messages (%d): %s ...", p_messages.length, p_messages[0]);
        // #endif /* LOGGER == TRACE */

        if (!p_messages[0].isExclusive()) {
            m_defaultMessageHandlerPool.newMessages(p_messages);
        } else {
            m_exclusiveMessageHandler.newMessages(p_messages);
        }
    }

    /**
     * Close the message handlers
     */
    void close() {
        // Shutdown default message handler(s)
        m_defaultMessageHandlerPool.shutdown();

        // Shutdown exclusive message handler
        m_exclusiveMessageHandler.shutdown();
    }
}
