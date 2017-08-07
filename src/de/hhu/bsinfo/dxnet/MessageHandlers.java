package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.Message;

/**
 * Created by nothaas on 6/12/17.
 */
public final class MessageHandlers {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandlers.class.getSimpleName());

    private final DefaultMessageHandlerPool m_defaultMessageHandlerPool;
    private final ExclusiveMessageHandler m_exclusiveMessageHandler;

    MessageHandlers(final int p_numMessageHandlerThreads, final MessageReceiverStore p_messageReceivers) {
        m_defaultMessageHandlerPool = new DefaultMessageHandlerPool(p_messageReceivers, p_numMessageHandlerThreads);

        m_exclusiveMessageHandler = new ExclusiveMessageHandler(p_messageReceivers);
        m_exclusiveMessageHandler.setName("Network: ExclusiveMessageHandler");
        m_exclusiveMessageHandler.start();
    }

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

    void close() {
        // Shutdown default message handler(s)
        m_defaultMessageHandlerPool.shutdown();

        // Shutdown exclusive message handler
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
}
