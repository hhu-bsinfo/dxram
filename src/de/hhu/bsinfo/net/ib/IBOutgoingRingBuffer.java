package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;

import de.hhu.bsinfo.net.core.AbstractMessage;
import de.hhu.bsinfo.net.core.AbstractOutgoingRingBuffer;
import de.hhu.bsinfo.net.core.NetworkException;
import de.hhu.bsinfo.utils.ByteBufferHelper;

/**
 * Created by nothaas on 7/17/17.
 */
public class IBOutgoingRingBuffer extends AbstractOutgoingRingBuffer {

    // struct NextWorkParameters
    // {
    //    uint64_t m_ptrBuffer;
    //    uint32_t m_len;
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

    // -1 if nothing available, otherwise unsafe address with data to send stored there
    long popFront(final IBConnection p_connection) {
        long tmp;
        int posBackRelative;
        int posFrontRelative;

        tmp = popFrontShift();
        posBackRelative = (int) (tmp >> 32 & 0x7FFFFFFF);
        posFrontRelative = (int) (tmp & 0x7FFFFFFF);

        // Empty
        if (posBackRelative == posFrontRelative) {
            return -1;
        }

        // assemble arguments for struct to pass back to jni
        ByteBuffer arguments = ms_sendWorkParameterPool.getInstance();

        arguments.clear();
        // memory start address of data to write
        arguments.putLong(m_bufferAddr + posFrontRelative);
        // length of data
        arguments.putInt(posBackRelative - posFrontRelative);
        // flow control data
        arguments.putInt(p_connection.getPipeOut().getFlowControlToWrite());
        // node id
        arguments.putShort(p_connection.getDestinationNodeID());

        return ByteBufferHelper.getDirectAddress(arguments);
    }

    @Override
    protected void serialize(final AbstractMessage p_message, final int p_start, final int p_messageSize, final boolean p_hasOverflow) throws NetworkException {
        IBMessageExporter exporter = (IBMessageExporter) IBExporterPool.getInstance().getExporter(p_hasOverflow);
        exporter.setBuffer(m_bufferAddr, m_bufferSize);
        exporter.setPosition(p_start);

        p_message.serialize(exporter, p_messageSize);
    }
}
