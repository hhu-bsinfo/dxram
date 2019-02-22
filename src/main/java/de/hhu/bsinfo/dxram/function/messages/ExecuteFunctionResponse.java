package de.hhu.bsinfo.dxram.function.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ClassUtil;
import de.hhu.bsinfo.dxutils.serialization.Distributable;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ExecuteFunctionResponse extends Response {

    private boolean m_hasResult;
    private String m_class;
    private Distributable m_result;

    public ExecuteFunctionResponse() {
        super();
    }

    public ExecuteFunctionResponse(final Request p_request, final Distributable p_result) {
        super(p_request, FunctionMessages.SUBTYPE_EXECUTE_FUNCTION_RESPONSE);
        m_result = p_result;
        m_hasResult = p_result != null;

        if (m_hasResult) {
            m_class = m_result.getClass().getName();
        }
    }

    public boolean hasResult() {
        return m_hasResult;
    }

    public Distributable getResult() {
        return m_result;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeBoolean(m_hasResult);

        if (!m_hasResult) {
            return;
        }

        p_exporter.writeString(m_class);
        p_exporter.exportObject(m_result);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_hasResult = p_importer.readBoolean(m_hasResult);

        if (!m_hasResult) {
            return;
        }

        m_class = p_importer.readString(m_class);

        if (m_result == null) {
            m_result = ClassUtil.createInstance(m_class);
        }

        p_importer.importObject(m_result);
    }

    @Override
    protected final int getPayloadLength() {
        if (m_hasResult) {
            return ObjectSizeUtil.sizeofBoolean() + ObjectSizeUtil.sizeofString(m_class) + m_result.sizeofObject();
        }

        return ObjectSizeUtil.sizeofBoolean();
    }

}
