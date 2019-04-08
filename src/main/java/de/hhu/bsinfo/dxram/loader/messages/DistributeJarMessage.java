package de.hhu.bsinfo.dxram.loader.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import lombok.Getter;

public class DistributeJarMessage extends Message {
    @Getter
    private byte[] m_jarBytes;
    @Getter
    private String m_jarName;

    public DistributeJarMessage(){
        super();
    }

    public DistributeJarMessage(final short p_destination, final String p_jarName, final byte[] p_jarBytes) {
        super(p_destination, DXRAMMessageTypes.LOADER_MESSAGE_TYPE, LoaderMessages.SUBTYPE_CLASS_DISTRIBUTE);
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
