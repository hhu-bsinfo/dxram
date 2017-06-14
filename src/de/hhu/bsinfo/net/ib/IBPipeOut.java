package de.hhu.bsinfo.net.ib;

import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractPipeOut;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBPipeOut extends AbstractPipeOut {
    private static final Logger LOGGER = LogManager.getFormatterLogger(AbstractPipeOut.class.getSimpleName());

    public IBPipeOut(final short p_ownNodeId, final short p_destinationNodeId, final int p_bufferSize, final AbstractFlowControl p_flowControl) {
        super(p_ownNodeId, p_destinationNodeId, p_bufferSize, p_flowControl);
    }

    @Override
    protected boolean isOpen() {
        return true;
    }

    @Override
    protected void bufferPosted() {
        ByteBuffer buffer;

        while (true) {
            buffer = getOutgoingQueue().popFront();

            if (buffer == null) {
                break;
            }

            if (!buffer.isDirect()) {
                throw new IllegalStateException("Buffer _MUST_ be direct for InfiniBand");
            }
            
            // #if LOGGER == TRACE
            LOGGER.trace("Posting buffer (%d) to 0x%X", buffer.limit(), getDestinationNodeID());
            // #endif /* LOGGER == TRACE */

            if (!JNIIbnet.postBuffer(getDestinationNodeID(), buffer, buffer.limit())) {
                // #if LOGGER == ERROR
                LOGGER.error("Posting buffer (%d) to 0x%X failed", buffer.limit(), getDestinationNodeID());
                // #endif /* LOGGER == ERROR */
            }
        }
    }
}
