package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Response;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class ClassResponseMessage extends Response {
    @Getter
    private byte[] m_jarBytes;
    @Getter
    private String m_jarName;

    public ClassResponseMessage(){
        super();
    }

    public ClassResponseMessage(final ClassRequestMessage p_request, final String p_jarName, final byte[] p_jarBytes) {
        super(p_request, LoaderMessages.SUBTYPE_CLASS_RESPONSE);
        m_jarName = p_jarName;
        m_jarBytes = p_jarBytes;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += ObjectSizeUtil.sizeofByteArray(m_jarBytes);
        size += ObjectSizeUtil.sizeofString(m_jarName);

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeByteArray(m_jarBytes);
        p_exporter.writeString(m_jarName);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_jarBytes = p_importer.readByteArray(m_jarBytes);
        m_jarName = p_importer.readString(m_jarName);
    }
}
