package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractOutgoingRingBuffer;
import de.hhu.bsinfo.net.core.ExporterPool;
import de.hhu.bsinfo.net.core.NetworkException;

/**
 * Created by nothaas on 7/17/17.
 */
public class IBOutgoingRingBuffer extends AbstractOutgoingRingBuffer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBOutgoingRingBuffer.class.getSimpleName());

    private final long m_bufferAddr;
    private final int m_bufferSize;

    IBOutgoingRingBuffer(final long p_bufferAddr, final int p_bufferSize) {
        super(p_bufferSize);

        m_bufferAddr = p_bufferAddr;
        m_bufferSize = p_bufferSize;
    }

    // 0 if nothing available, otherwise unsafe address with data to send stored there
    long popFront() {
        return popFrontShift();
    }

    @Override
    protected void serialize(final AbstractMessage p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {
        AbstractMessageExporter exporter = ExporterPool.getInstance().getExporter(p_hasOverflow);
        exporter.setBuffer(m_bufferAddr, m_bufferSize);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
    }
}
