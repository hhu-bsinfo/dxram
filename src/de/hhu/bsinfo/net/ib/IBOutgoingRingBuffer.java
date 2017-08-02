package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractExporterPool;
import de.hhu.bsinfo.net.core.OutgoingRingBuffer;

/**
 * Implementation of the outgoing ring buffer for IB
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.07.2017
 */
class IBOutgoingRingBuffer extends OutgoingRingBuffer {

    /**
     * Constructor
     *
     * @param p_bufferAddr
     *         Unsafe address of the ring buffer
     * @param p_bufferSize
     *         Size of the buffer
     * @param p_exporterPool
     *         Exporter pool instance
     */
    IBOutgoingRingBuffer(final long p_bufferAddr, final int p_bufferSize, final AbstractExporterPool p_exporterPool) {
        super(p_exporterPool);

        setBuffer(p_bufferAddr, p_bufferSize);
    }

    /**
     * Get the next currently available slice of data to send out
     *
     * @return Long holding the current relative position of the front pointer
     * (lower 32-bit) and the relative position of the back pointer
     * (higher 32-bit) of the ring buffer
     */
    long popFront() {
        return popFrontShift();
    }
}
