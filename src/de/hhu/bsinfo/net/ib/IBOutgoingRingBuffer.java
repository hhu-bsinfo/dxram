package de.hhu.bsinfo.net.ib;

import de.hhu.bsinfo.net.core.AbstractExporterPool;
import de.hhu.bsinfo.net.core.OutgoingRingBuffer;

/**
 * Created by nothaas on 7/17/17.
 */
class IBOutgoingRingBuffer extends OutgoingRingBuffer {

    IBOutgoingRingBuffer(final long p_bufferAddr, final int p_bufferSize, AbstractExporterPool p_exporterPool) {
        super(p_exporterPool);
        
        setBuffer(p_bufferAddr, p_bufferSize);
    }

    long popFront() {
        return popFrontShift();
    }
}
