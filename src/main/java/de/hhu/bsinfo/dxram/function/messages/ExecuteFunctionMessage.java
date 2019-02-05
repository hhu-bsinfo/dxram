package de.hhu.bsinfo.dxram.function.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.function.DistributableFunction;
import de.hhu.bsinfo.dxram.function.util.FunctionSerializer;
import de.hhu.bsinfo.dxram.job.messages.JobMessages;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ExecuteFunctionMessage extends Message {

    private String m_name;

    public ExecuteFunctionMessage() {
        super();
    }

    public ExecuteFunctionMessage(final short p_destination, final String p_name) {
        super(p_destination, DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION);

        m_name = p_name;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_name);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_name = p_importer.readString(m_name);
    }

    public String getName() {
        return m_name;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofString(m_name);
    }
}