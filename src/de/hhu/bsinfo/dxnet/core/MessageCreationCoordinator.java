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

package de.hhu.bsinfo.dxnet.core;

import java.util.concurrent.locks.LockSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The MessageCreationCoordinator builds messages and forwards them to the MessageHandlers.
 *
 * @author Kevin Beineke, kevin.beineke@hhu.de, 31.05.2016
 */
public class MessageCreationCoordinator extends Thread {
    private static final Logger LOGGER = LogManager.getFormatterLogger(MessageCreationCoordinator.class.getSimpleName());

    // optimized values determined by experiments
    private static final int THRESHOLD_PARK = 10000;

    private IncomingBufferQueue m_bufferQueue;

    private volatile boolean m_shutdown;

    /**
     * Creates an instance of MessageCreationCoordinator
     *
     * @param p_maxIncomingBufferSize
     *         the max incoming buffer size
     */
    public MessageCreationCoordinator(final int p_maxIncomingBufferSize) {
        m_bufferQueue = new IncomingBufferQueue(p_maxIncomingBufferSize);
    }

    /**
     * Returns the incoming buffer queue
     *
     * @return the IncomingBufferQueue
     */
    public IncomingBufferQueue getIncomingBufferQueue() {
        return m_bufferQueue;
    }

    @Override
    public void run() {
        IncomingBufferQueue.IncomingBuffer incomingBuffer;
        //int parkCounter = 0;

        // TODO: Idle
        while (!m_shutdown) {
            // pop an incomingBuffer
            incomingBuffer = m_bufferQueue.popBuffer();
            if (incomingBuffer == null) {
                // Ring-buffer is empty.

                // Wait for a short period (~ xx µs) and continue
                // keep latency low (especially on infiniband) but also keep cpu load low
                // avoid parking on every iteration -> increases overall latency for messages
                /*if (parkCounter >= THRESHOLD_PARK) {
                    LockSupport.parkNanos(1);
                } else {
                    parkCounter++;
                }*/
                Thread.yield();
            } else {
                //parkCounter = 0;

                try {
                    incomingBuffer.getPipeIn().processBuffer(incomingBuffer);
                } catch (final NetworkException e) {
                    incomingBuffer.getPipeIn().returnProcessedBuffer(incomingBuffer.getDirectBuffer(), incomingBuffer.getBufferHandle());

                    // #if LOGGER == ERROR
                    LOGGER.error("Processing incoming buffer failed", e);
                    // #endif /* LOGGER == ERROR */
                }
            }
        }
    }

    /**
     * Shutdown the message creator thread
     */
    public void shutdown() {
        // #if LOGGER == INFO
        LOGGER.info("Message creator shutdown...");
        // #endif /* LOGGER == INFO */

        m_shutdown = true;

        try {
            // wait a moment for the thread to shut down (if it can)
            Thread.sleep(100);
        } catch (final InterruptedException ignore) {

        }

        interrupt();
        LockSupport.unpark(this);
        try {
            join();
        } catch (final InterruptedException ignore) {
        }
    }

}
