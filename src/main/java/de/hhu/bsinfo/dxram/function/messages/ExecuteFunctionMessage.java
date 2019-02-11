package de.hhu.bsinfo.dxram.function.messages;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxram.function.DistributableFunction;
import de.hhu.bsinfo.dxram.function.util.FunctionSerializer;
import de.hhu.bsinfo.dxram.job.messages.JobMessages;
import de.hhu.bsinfo.dxutils.serialization.ClassUtil;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.Exportable;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ExecuteFunctionMessage extends Message {

    private String m_name;
    private String m_class;
    private Distributable m_input;

    public ExecuteFunctionMessage() {
        super();
    }

    public ExecuteFunctionMessage(final short p_destination, final String p_name, final Distributable p_input) {
        super(p_destination, DXRAMMessageTypes.FUNCTION_MESSAGE_TYPE, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION);

        m_name = p_name;
        m_input = p_input;
        m_class = p_input.getClass().getCanonicalName();
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_name);
        p_exporter.writeString(m_class);
        p_exporter.exportObject(m_input);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_name = p_importer.readString(m_name);
        m_class = p_importer.readString(m_class);

        if (m_input == null) {
            m_input = ClassUtil.createInstance(m_class);
        }

        p_importer.importObject(m_input);
    }

    public String getName() {
        return m_name;
    }

    public Distributable getInput() {
        return m_input;
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofString(m_name) + m_input.sizeofObject() + ObjectSizeUtil.sizeofString(m_class);
    }
}