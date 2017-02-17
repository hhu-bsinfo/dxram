package de.hhu.bsinfo.dxcompute.bench;

import de.hhu.bsinfo.dxcompute.DXComputeMessageTypes;
import de.hhu.bsinfo.dxcompute.ms.messages.MasterSlaveMessages;
import de.hhu.bsinfo.ethnet.AbstractMessage;

import java.nio.ByteBuffer;

/**
 * Created by akguel on 25.01.17.
 */
public class NetworkTestMessage extends AbstractMessage {

    private byte[] m_data;

    /**
     * Creates an instance of TaskRemoteCallbackMessage.
     * This constructor is used when receiving this message.
     */
    public NetworkTestMessage() {
        super();
    }

    /**
     * Creates an instance of TaskRemoteCallbackMessage.
     * This constructor is used when sending this message.
     * @param p_destination
     *            the destination node id.
     * @param p_messageSize
     *            Size of byte array.
     */
    public NetworkTestMessage(final short p_destination, final int p_messageSize) {
        super(p_destination, DXComputeMessageTypes.BENCH_MESSAGE_TYPE, BenchMessages.NETWORK_TEST_MESSAGE);

        m_data = new byte[p_messageSize];
    }


    @Override
    protected final void writePayload(final ByteBuffer p_buffer) {
        p_buffer.putInt(m_data.length);
        p_buffer.put(m_data);
    }

    @Override
    protected final void readPayload(final ByteBuffer p_buffer) {
        int length = p_buffer.getInt();
        m_data = new byte[length];
        p_buffer.get(m_data, 0, length);
    }

    @Override
    protected final int getPayloadLength() {
        return Integer.BYTES + m_data.length * Byte.BYTES;
    }
}
