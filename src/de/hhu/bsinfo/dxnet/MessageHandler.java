/*
 * Copyright (C) 2017 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package de.hhu.bsinfo.dxnet;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.LocalMessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.MessageHeader;
import de.hhu.bsinfo.dxnet.core.MessageHeaderPool;
import de.hhu.bsinfo.dxnet.core.MessageImporterCollection;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.utils.stats.StatisticsOperation;
import de.hhu.bsinfo.utils.stats.StatisticsRecorderManager;

/**
 * Executes incoming default messages
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 19.07.2016
 */
final class MessageHandler extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageHandler.class.getSimpleName());
    private static final StatisticsOperation SOP_POP = StatisticsRecorderManager.getOperation(MessageHandler.class, "Pop");
    private static final StatisticsOperation SOP_WAIT = StatisticsRecorderManager.getOperation(MessageHandler.class, "Wait");
    private static final StatisticsOperation SOP_SLEEP = StatisticsRecorderManager.getOperation(MessageHandler.class, "Sleep");
    private static final StatisticsOperation SOP_CREATE = StatisticsRecorderManager.getOperation(MessageHandler.class, "Create");
    private static final StatisticsOperation SOP_EXECUTE = StatisticsRecorderManager.getOperation(MessageHandler.class, "Execute");

    // optimized values determined by experiments
    private static final int THRESHOLD_PARK = 10;
    private static final int THRESHOLD_PARK_SLEEP = 100;
    //private static final int THRESHOLD_PARK = 10000;
    //private static final int THRESHOLD_PARK_SLEEP = 1000;

    private final MessageReceiverStore m_messageReceivers;
    private final MessageHeaderStore m_defaultMessages;
    private final MessageImporterCollection m_importers;
    private final LocalMessageHeaderPool m_messageHeaderPool;
    private volatile boolean m_shutdown;

    // Constructors

    /**
     * Creates an instance of MessageHandler
     *
     * @param p_queue
     *         the message queue
     */
    MessageHandler(final MessageReceiverStore p_messageReceivers, final MessageHeaderStore p_queue, final MessageHeaderPool p_messageHeaderPool) {
        m_messageReceivers = p_messageReceivers;
        m_defaultMessages = p_queue;
        m_importers = new MessageImporterCollection();
        m_messageHeaderPool = new LocalMessageHeaderPool(p_messageHeaderPool);
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
        //int waitCounter = 0;
        //int sleepCounter = 0;
        MessageHeader header;
        Message message;
        MessageReceiver messageReceiver;

        while (!m_shutdown) {
            // #ifdef STATISTICS
            // SOP_POP.enter();
            // #endif /* STATISTICS */

            header = m_defaultMessages.popMessageHeader();

            if (header == null) {
                // keep latency low (especially on infiniband) but also keep cpu load low
                // avoid parking on every iteration -> increases overall latency for messages
                //if (sleepCounter > THRESHOLD_PARK_SLEEP) {
                // #ifdef STATISTICS
                // SOP_WAIT.enter();
                // #endif /* STATISTICS */

                // No new message for a longer period -> increase sleep to 1 ms to reduce cpu load
                // continue sleeping until new messages are available
                //LockSupport.parkNanos(1000 * 1000);

                // #ifdef STATISTICS
                // SOP_WAIT.leave();
                // #endif /* STATISTICS */
                //} else if (waitCounter > THRESHOLD_PARK) {
                // #ifdef STATISTICS
                // SOP_SLEEP.enter();
                // #endif /* STATISTICS */

                // No new message at the moment -> sleep for xx µs and try again
                LockSupport.parkNanos(1);
                //sleepCounter++;

                // #ifdef STATISTICS
                // SOP_SLEEP.leave();
                // #endif /* STATISTICS */
                //} else {
                //waitCounter++;
                //}

                // #ifdef STATISTICS
                // SOP_POP.leave();
                // #endif /* STATISTICS */

                continue;
            }

            // #ifdef STATISTICS
            // SOP_POP.leave();
            // #endif /* STATISTICS */

            // reset waits and sleeps
            //waitCounter = 0;
            //sleepCounter = 0;

            // #ifdef STATISTICS
            // SOP_CREATE.enter();
            // #endif /* STATISTICS */

            try {
                message = header.createAndFillMessage(m_importers, m_messageHeaderPool);
            } catch (NetworkException e) {
                e.printStackTrace();
                continue;
            }

            // #ifdef STATISTICS
            // SOP_CREATE.leave();
            // #endif /* STATISTICS */

            if (message != null) {
                messageReceiver = m_messageReceivers.getReceiver(message.getType(), message.getSubtype());

                if (messageReceiver != null) {
                    // #ifdef STATISTICS
                    // SOP_EXECUTE.enter();
                    // #endif /* STATISTICS */

                    messageReceiver.onIncomingMessage(message);

                    // #ifdef STATISTICS
                    // SOP_EXECUTE.leave();
                    // #endif /* STATISTICS */
                } else {
                    // #if LOGGER >= ERROR
                    LOGGER.error("No message receiver was registered for %d, %d!", message.getType(), message.getSubtype());
                    // #endif /* LOGGER >= ERROR */
                }
            }
        }
    }
}
