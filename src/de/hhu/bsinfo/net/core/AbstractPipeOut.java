package de.hhu.bsinfo.net.core;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by nothaas on 6/9/17.
 */
public abstract class AbstractPipeOut {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeOut.class.getSimpleName());

    private final short m_ownNodeID;
    private final short m_destinationNodeID;
    private volatile boolean m_isConnected;
    private final int m_bufferSize;
    private final AbstractFlowControl m_flowControl;
    private final OutgoingRingBuffer m_outgoing;

    private final ReentrantLock m_sliceLock;

    // TODO needs to be atomic now because lock is missing?
    private long m_sentMessages;

    public AbstractPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final int p_bufferSize, final AbstractFlowControl p_flowControl,
            final boolean p_directBuffer) {
        m_ownNodeID = p_ownNodeId;
        m_destinationNodeID = p_destinationNodeId;

        m_bufferSize = p_bufferSize;
        m_flowControl = p_flowControl;

        m_outgoing = new OutgoingRingBuffer(m_bufferSize, p_directBuffer);

        m_sliceLock = new ReentrantLock(false);

        m_sentMessages = 0;
    }

    short getOwnNodeID() {
        return m_ownNodeID;
    }

    public short getDestinationNodeID() {
        return m_destinationNodeID;
    }

    protected int getBufferSize() {
        return m_bufferSize;
    }

    public boolean isConnected() {
        return m_isConnected;
    }

    public void setConnected(final boolean p_connected) {
        m_isConnected = p_connected;
    }

    public long getSentMessageCount() {
        return m_sentMessages;
    }

    protected AbstractFlowControl getFlowControl() {
        return m_flowControl;
    }

    public boolean isOutgoingQueueEmpty() {
        return m_outgoing.isEmpty();
    }

    public void postMessage(final AbstractMessage p_message, final boolean p_directBuffer) throws NetworkException {
        // #if LOGGER >= TRACE
        LOGGER.trace("Writing message %s to pipe out of dest 0x%X", p_message, m_destinationNodeID);
        // #endif /* LOGGER >= TRACE */

        int messageTotalSize = p_message.getTotalSize();
        m_flowControl.dataToSend(messageTotalSize);
        m_sentMessages++;

        // TODO: Large messages
        /*if (messageTotalSize > m_bufferSize) {
            ByteBuffer data = p_message.getBuffer(p_directBuffer);
            m_sliceLock.lock();
            int size = data.limit();
            int currentSize = 0;
            while (currentSize < size) {
                currentSize = Math.min(currentSize + m_bufferSize, size);
                data.limit(currentSize);
                postBuffer(data.slice());
                data.position(currentSize);
            }
            m_sliceLock.unlock();
        } else {*/
            postBuffer(p_message, messageTotalSize);
        //}
    }

    public void postBuffer(final ByteBuffer p_buffer) throws NetworkException {
        System.out.println("Warning: Large messages currently not supported!");
        m_outgoing.pushNodeID(p_buffer);
        bufferPosted();
    }

    private void postBuffer(final AbstractMessage p_message, final int p_messageSize) throws NetworkException {
        m_outgoing.pushMessage(p_message, p_messageSize);
        bufferPosted();
    }

    protected abstract boolean isOpen();

    protected abstract void bufferPosted();

    protected OutgoingRingBuffer getOutgoingQueue() {
        return m_outgoing;
    }
}
