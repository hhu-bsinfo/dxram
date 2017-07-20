package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractMessageExporter;
import de.hhu.bsinfo.net.core.AbstractOutgoingRingBuffer;
import de.hhu.bsinfo.net.core.ExporterPool;
import de.hhu.bsinfo.net.core.NetworkException;
import de.hhu.bsinfo.utils.ByteBufferHelper;

/**
 * Created by nothaas on 7/17/17.
 */
public class IBOutgoingRingBuffer extends AbstractOutgoingRingBuffer {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBOutgoingRingBuffer.class.getSimpleName());

    // struct NextWorkParameters
    // {
    //    uint32_t m_posFrontRel;
    //    uint32_t m_posBackRel;
    //    uint32_t m_flowControlData;
    //    uint16_t m_nodeId;
    //} __attribute__((packed));
    private static final IBSendWorkParameterPool ms_sendWorkParameterPool =
            new IBSendWorkParameterPool(Long.BYTES + Integer.BYTES + Integer.BYTES + Short.BYTES);

    private final long m_bufferAddr;
    private final int m_bufferSize;

    IBOutgoingRingBuffer(final long p_bufferAddr, final int p_bufferSize) {
        super(p_bufferSize);

        m_bufferAddr = p_bufferAddr;
        m_bufferSize = p_bufferSize;
    }

    // 0 if nothing available, otherwise unsafe address with data to send stored there
    long popFront(final IBConnection p_connection) {
        long tmp;
        int posBackRelative;
        int posFrontRelative;

        tmp = popFrontShift();
        posBackRelative = (int) (tmp >> 32 & 0x7FFFFFFF);
        posFrontRelative = (int) (tmp & 0x7FFFFFFF);

        // Empty
        if (posBackRelative == posFrontRelative) {
            return 0;
        }

        // assemble arguments for struct to pass back to jni
        ByteBuffer arguments = ms_sendWorkParameterPool.getInstance();

        arguments.clear();
        // relative position of data start in buffer
        arguments.putInt(posFrontRelative);
        // relative position of data end in buffer
        arguments.putInt(posBackRelative);
        // flow control data
        arguments.putInt(p_connection.getPipeOut().getFlowControlToWrite());
        // node id
        arguments.putShort(p_connection.getDestinationNodeID());

        // #if LOGGER >= TRACE
        LOGGER.trace("Next write on node 0x%X, posFrontRelative %d, posBackRelative %d", p_connection.getDestinationNodeID(), posFrontRelative,
                posBackRelative);
        // #endif /* LOGGER >= TRACE */

        return ByteBufferHelper.getDirectAddress(arguments);
    }

    @Override
    protected void serialize(final AbstractMessage p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {
        AbstractMessageExporter exporter = ExporterPool.getInstance().getExporter(p_hasOverflow);
        exporter.setBuffer(m_bufferAddr, m_bufferSize);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
    }
}
