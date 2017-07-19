package de.hhu.bsinfo.net.ib;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.net.core.AbstractFlowControl;
import de.hhu.bsinfo.net.core.AbstractMessageImporter;
import de.hhu.bsinfo.net.core.AbstractPipeIn;
import de.hhu.bsinfo.net.core.DataReceiver;
import de.hhu.bsinfo.net.core.MessageDirectory;
import de.hhu.bsinfo.net.core.NetworkException;
import de.hhu.bsinfo.net.core.RequestMap;

/**
 * Created by nothaas on 6/13/17.
 */
public class IBPipeIn extends AbstractPipeIn {
    private static final Logger LOGGER = LogManager.getFormatterLogger(IBPipeIn.class.getSimpleName());

    private IBMessageImporterCollection m_importers;

    private long m_currentAddr;
    private int m_currentSize;
    private int m_currentPosition;

    public IBPipeIn(final short p_ownNodeId, final short p_destinationNodeId, final AbstractFlowControl p_flowControl,
            final MessageDirectory p_messageDirectory, final RequestMap p_requestMap, final DataReceiver p_dataReceiver) {
        super(p_ownNodeId, p_destinationNodeId, p_flowControl, p_messageDirectory, p_requestMap, p_dataReceiver, true);

        m_importers = new IBMessageImporterCollection();
    }

    public void handleFlowControlData(final int p_confirmedBytes) {
        getFlowControl().handleFlowControlData(p_confirmedBytes);
    }

    public void processBuffer(final long p_addr, final int p_length) throws NetworkException {
        m_currentAddr = p_addr;
        m_currentSize = p_length;
        m_currentPosition = 0;

        processBuffer(p_length);
    }

    public void returnProcessedBuffer(final long p_addr, final int p_size) {
        // TODO return to IB context
    }

    @Override
    public boolean isOpen() {
        return true;
    }

    @Override
    protected AbstractMessageImporter getImporter(final boolean p_overflow) {
        IBMessageImporter importer;
        importer = (IBMessageImporter) m_importers.getImporter(m_currentAddr, m_currentSize, m_currentPosition, p_overflow);
        return importer;
    }

    @Override
    protected void returnImporter(final AbstractMessageImporter p_importer, final boolean p_finished) {
        // update known position before returning importer
        m_currentPosition += p_importer.getNumberOfReadBytes();
        m_importers.returnImporter(p_importer, true);
    }
}
