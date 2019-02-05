package de.hhu.bsinfo.dxram.function.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Request;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RegisterFunctionResponse extends Response {

    private boolean m_isRegistered = false;

    public RegisterFunctionResponse() {
        super();
    }

    public RegisterFunctionResponse(final Request p_request, final boolean p_isRegistered) {
        super(p_request, FunctionMessages.SUBTYPE_REGISTER_FUNCTION_RESPONSE);
        m_isRegistered = p_isRegistered;
    }

    public boolean isRegistered() {
        return m_isRegistered;
    }

    @Override
    protected final void writePayload(final AbstractMessageExporter p_exporter) {
        p_exporter.writeBoolean(m_isRegistered);
    }

    @Override
    protected final void readPayload(final AbstractMessageImporter p_importer) {
        m_isRegistered = p_importer.readBoolean(m_isRegistered);
    }

    @Override
    protected final int getPayloadLength() {
        return ObjectSizeUtil.sizeofBoolean();
    }

}
