package de.hhu.bsinfo.net.ib;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.utils.NodeID;

/**
 * Write interests for a single connection. This keeps track of available data on the outgoing buffer
 * to tell the (IB) send thread if there is data to send on any connection.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 02.08.2017
 */
class IBWriteInterest {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBWriteInterest.class.getSimpleName());

    private final short m_nodeId;
    private AtomicLong m_interestsAvailable;

    IBWriteInterest(final short p_nodeId) {
        m_nodeId = p_nodeId;
        m_interestsAvailable = new AtomicLong(0);
    }

    @Override
    public String toString() {
        long tmp = m_interestsAvailable.get();
        return NodeID.toHexString(m_nodeId) + ", " + (tmp & 0x7FFFFFFF) + ", " + (tmp >> 32);
    }

    short getNodeId() {
        return m_nodeId;
    }

    boolean addDataInterest() {
        long tmp;
        boolean ret;

        while (true) {
            tmp = m_interestsAvailable.get();
            ret = tmp == 0;

            if (m_interestsAvailable.weakCompareAndSet(tmp, tmp + 1)) {
                break;
            }
        }

        return ret;
    }

    boolean addFcInterest() {
        long tmp;
        boolean ret;

        while (true) {
            tmp = m_interestsAvailable.get();
            ret = tmp == 0;

            if (m_interestsAvailable.weakCompareAndSet(tmp, tmp + (1L << 32))) {
                break;
            }
        }

        return ret;
    }

    // lower 32-bit data, higher fc interests
    long consumeInterests() {
        return m_interestsAvailable.getAndSet(0);
    }

    // on node disconnect, only
    void reset() {
        m_interestsAvailable.set(0);
    }
}
