package de.hhu.bsinfo.dxram.loader.messages;

import lombok.Getter;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class RegisterJarMessage extends Message {
    @Getter
    private String m_jarName;
    @Getter
    private byte[] m_jarBytes;

    public RegisterJarMessage(){
        super();
    }

    public RegisterJarMessage(final short p_destination, final String p_jarName, final byte[] p_jarBytes) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_REGISTER);
        m_jarName = p_jarName;
        m_jarBytes = p_jarBytes;
    }

    @Override
    protected final int getPayloadLength() {
        int size = 0;

        size += ObjectSizeUtil.sizeofString(m_jarName);
        size += ObjectSizeUtil.sizeofByteArray(m_jarBytes);

        return size;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeString(m_jarName);
        p_exporter.writeByteArray(m_jarBytes);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        m_jarName = p_importer.readString(m_jarName);
        m_jarBytes = p_importer.readByteArray(m_jarBytes);
    }
}
